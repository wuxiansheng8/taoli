const { ApiPromise, WsProvider, Keyring } = require('@polkadot/api');
const { cryptoWaitReady } = require('@polkadot/util-crypto');
const axios = require('axios');
const WebSocket = require('ws');
const database = require('./database');

// Logs buffer and state
const logs = [];
const maxLogs = 2000;
let api = null;
let provider = null;
let botStatus = 'Stopped'; // 'Stopped', 'Starting', 'Running', 'Error'
let currentActiveNode = '';
let currentLatency = -1;
let currentBlockHeight = 0;
let systemUptimeStart = null;
let pollTimer = null;
let latencyTimer = null;
let isPolling = false;
let reconnectTimer = null;
let isConnecting = false;
let connectGeneration = 0;
let wallets = [];
let keyring = null;
let lastMempoolErrorTime = 0; // added for throttled error logging

// Mempool maps for deduplication and nonce tracking
const seenHashes = new Map();
const seenActions = new Map();
const nextNonceByAddress = new Map();
const balanceByAddress = new Map();
const processedNetuids = new Map();
const maxTipBySubnet = new Map(); // added for dynamic tip bidding
let maxRegisterNetworkTip = 0; // added for dynamic registerNetwork tip bidding
const dashingSuccessByNetuid = new Map(); // added for tracking successful snipes
const activeSnipesByNetuid = new Set(); // added to prevent concurrent duplicate sniping loops

// Subnet Owner, Hotkey & Registration Block Cache (to bypass RPC network queries during critical frontrunning path)
const subnetOwnersCache = new Map();
const subnetHotkeysCache = new Map();
const subnetRegisteredAtCache = new Map();
let successfulSyncCount = 0;

// RBF & Multi-Node Broadcast State
const activePendingTxs = new Map(); // nonceKey -> pending details
const broadcastProviders = new Map(); // url -> WsProvider
const broadcastStatuses = new Map(); // url -> { status, latency }
const activeTimeoutRetryNumByWallet = new Map(); // netuid:walletName -> attemptNumber
let broadcastLatencyTimer = null;

function initBroadcastNodes() {
  const settings = database.getSettings();
  const nodes = settings.broadcastNodes || [];
  
  // Close old connections
  for (const provider of broadcastProviders.values()) {
    try { provider.disconnect(); } catch (e) {}
  }
  broadcastProviders.clear();
  broadcastStatuses.clear();
  
  log('INFO', `正在初始化 ${nodes.length} 个备用广播节点...`);
  for (const url of nodes) {
    if (!url) continue;
    try {
      const prov = new WsProvider(url, false);
      broadcastProviders.set(url, prov);
      broadcastStatuses.set(url, { status: 'Disconnected', latency: -1 });
      
      prov.on('connected', () => {
        broadcastStatuses.set(url, { status: 'Connected', latency: -1 });
      });
      prov.on('disconnected', () => {
        broadcastStatuses.set(url, { status: 'Disconnected', latency: -1 });
      });
      prov.on('error', () => {
        broadcastStatuses.set(url, { status: 'Disconnected', latency: -1 });
      });
      
      prov.connect();
    } catch (err) {
      log('WARN', `初始化广播节点 ${url} 失败: ${err.message}`);
    }
  }
}

async function testBroadcastNodes() {
  // Check if settings have changed
  const settings = database.getSettings();
  const configuredNodes = settings.broadcastNodes || [];
  let changed = false;
  if (configuredNodes.length !== broadcastProviders.size) {
    changed = true;
  } else {
    for (const url of configuredNodes) {
      if (!broadcastProviders.has(url)) {
        changed = true;
        break;
      }
    }
  }
  
  if (changed) {
    initBroadcastNodes();
  }

  const promises = [];
  for (const [url, provider] of broadcastProviders.entries()) {
    const start = Date.now();
    const testPromise = new Promise(async (resolve) => {
      try {
        if (!provider.isConnected) {
          broadcastStatuses.set(url, { status: 'Disconnected', latency: -1 });
          return resolve();
        }
        await provider.send('system_health', []);
        const lat = Date.now() - start;
        broadcastStatuses.set(url, { status: 'Connected', latency: lat });
      } catch (e) {
        broadcastStatuses.set(url, { status: 'Disconnected', latency: -1 });
      }
      resolve();
    });
    promises.push(testPromise);
  }
  await Promise.allSettled(promises);
  
  if (global.blockCallback) {
    global.blockCallback(currentBlockHeight);
  }
}

function getBroadcastNodesStatus() {
  const list = [];
  for (const [url, status] of broadcastStatuses.entries()) {
    list.push({
      url,
      status: status.status,
      latency: status.latency
    });
  }
  return list;
}

function broadcastSignedTx(signedTxHex) {
  for (const [nodeUrl, provider] of broadcastProviders.entries()) {
    const status = broadcastStatuses.get(nodeUrl);
    if (status && status.status === 'Connected') {
      provider.send('author_submitExtrinsic', [signedTxHex]).catch((err) => {
        // Silent catch for broadcast failures
      });
    }
  }
}

async function calculateDynamicSlippage(netuid, victimAmountTao) {
  try {
    const settings = database.getSettings();
    if (!settings.dynamicSlippageEnabled) {
      return settings.sandwichSlippageLimit || 0.05;
    }

    const priceBig = await getSubnetPrice(netuid);
    if (!priceBig) return settings.sandwichSlippageLimit || 0.05;
    const P0 = Number(priceBig); // Price in RAO TAO per RAO Alpha

    const t = 10 * 1e9; // Simulate swapping 10 TAO
    let aVal;
    if (api.rpc.swap && api.rpc.swap.simSwapTaoForAlpha) {
      aVal = await api.rpc.swap.simSwapTaoForAlpha(netuid, t);
    } else {
      return settings.sandwichSlippageLimit || 0.05;
    }
    
    const a = Number(aVal.toString());
    if (a <= 0) return settings.sandwichSlippageLimit || 0.05;

    const P_rao = P0 / 1e9;
    const denominator = t - a * P_rao;
    if (denominator <= 0) return settings.sandwichSlippageLimit || 0.05;
    
    const A = (a * t) / denominator;
    const T = P_rao * A;
    
    const V = victimAmountTao * 1e9;
    const priceRatio = Math.pow(1.0 + V / T, 2);
    const expectedRise = priceRatio - 1.0;
    
    const safetyFactor = settings.dynamicSlippageSafetyFactor || 0.7;
    const dynamicSlippage = expectedRise * safetyFactor;
    
    log('INFO', `[动态滑点] 子网 #${netuid} 推算储备金: ${(T / 1e9).toFixed(2)} TAO, 受害者交易: ${victimAmountTao} TAO, 预测价格涨幅: ${(expectedRise * 100).toFixed(2)}%, 设定动态滑点: ${(dynamicSlippage * 100).toFixed(2)}%`);
    
    return Math.max(0.01, Math.min(0.50, dynamicSlippage));
  } catch (err) {
    log('WARN', `[动态滑点] 计算异常: ${err.message}，回退使用固定滑点。`);
    const settings = database.getSettings();
    return settings.sandwichSlippageLimit || 0.05;
  }
}

async function refreshSubnetOwnersCache() {
  if (!api || !api.isConnected) return;
  try {
    const netuidKeys = await api.query.subtensorModule.networksAdded.keys();
    const activeNetuids = netuidKeys.map(({ args: [netuid] }) => netuid.toNumber());
    
    // 批量并发查询所有子网的 Owner 冷键、Owner Hotkey 以及注册区块号
    const [owners, ownerHotkeys, registeredBlocks] = await Promise.all([
      api.query.subtensorModule.subnetOwner.multi(activeNetuids),
      api.query.subtensorModule.subnetOwnerHotkey.multi(activeNetuids),
      api.query.subtensorModule.networkRegisteredAt.multi(activeNetuids)
    ]);
    
    const changes = [];
    const isFirstSync = subnetOwnersCache.size === 0;

    if (!isFirstSync) {
      // 1. 检测已有子网的属性变更或新增子网
      for (let i = 0; i < activeNetuids.length; i++) {
        const netuid = activeNetuids[i];
        const ownerStr = owners[i]?.toString();
        const hotkeyStr = ownerHotkeys[i]?.toString();
        const registeredBlock = Number(registeredBlocks[i]?.toString() || 0);

        const oldOwner = subnetOwnersCache.get(netuid);
        const oldHotkey = subnetHotkeysCache.get(netuid);
        const oldRegBlock = subnetRegisteredAtCache.get(netuid);

        if (oldOwner === undefined) {
          changes.push(`[子网变动] 检测到新子网上线：#${netuid} (Owner: ${ownerStr ? ownerStr.slice(0, 8) + '...' : '无'}, Hotkey: ${hotkeyStr ? hotkeyStr.slice(0, 8) + '...' : '无'}, 注册高度: ${registeredBlock})`);
        } else {
          if (ownerStr && oldOwner !== ownerStr) {
            changes.push(`[子网变动] 子网 #${netuid} 所有者 (Coldkey) 发生变更：${oldOwner.slice(0, 8)}... -> ${ownerStr.slice(0, 8)}...`);
          }
          if (hotkeyStr && hotkeyStr.length >= 47 && oldHotkey !== hotkeyStr) {
            changes.push(`[子网变动] 子网 #${netuid} 的 Hotkey 发生变更：${oldHotkey ? oldHotkey.slice(0, 8) + '...' : '无'} -> ${hotkeyStr.slice(0, 8)}...`);
          }
          if (registeredBlock > 0 && oldRegBlock !== registeredBlock) {
            changes.push(`[子网变动] 子网 #${netuid} 注册高度发生变更：${oldRegBlock || 0} -> ${registeredBlock} (可能被接管/回收)`);
          }
        }
      }

      // 2. 检测子网下线
      const newNetuidsSet = new Set(activeNetuids);
      for (const oldNetuid of subnetOwnersCache.keys()) {
        if (!newNetuidsSet.has(oldNetuid)) {
          changes.push(`[子网变动] 子网 #${oldNetuid} 已下线/删除`);
        }
      }
    }

    subnetOwnersCache.clear();
    subnetHotkeysCache.clear();
    subnetRegisteredAtCache.clear();
    for (let i = 0; i < activeNetuids.length; i++) {
      const netuid = activeNetuids[i];
      const ownerStr = owners[i]?.toString();
      const hotkeyStr = ownerHotkeys[i]?.toString();
      const registeredBlock = Number(registeredBlocks[i]?.toString() || 0);
      
      if (ownerStr) {
        subnetOwnersCache.set(netuid, ownerStr);
      }
      if (hotkeyStr && hotkeyStr.length >= 47) {
        subnetHotkeysCache.set(netuid, hotkeyStr);
      }
      if (registeredBlock > 0) {
        subnetRegisteredAtCache.set(netuid, registeredBlock);
      }
    }

    if (isFirstSync) {
      log('SUCCESS', `[缓存同步] 子网缓存初始化成功。已缓存 ${activeNetuids.length} 个子网的 Owner 账户、Hotkey 及注册高度信息。`);
    } else if (changes.length > 0) {
      for (const msg of changes) {
        log('SUCCESS', msg);
      }
      successfulSyncCount = 0; // 重置心跳计数
    } else {
      successfulSyncCount++;
      if (successfulSyncCount >= 10) {
        log('SUCCESS', `[缓存同步] 子网缓存运行正常（已连续 ${successfulSyncCount} 次同步无变化，心跳正常）。已缓存 ${activeNetuids.length} 个子网。`);
        successfulSyncCount = 0;
      }
    }
  } catch (e) {
    log('WARN', `[缓存同步] 自动同步子网 Owner 缓存失败: ${e.message}`);
  }
}

// Helper to log with Beijing Time (UTC+8)
function log(level, message) {
  const tzOffset = 8 * 60; // Beijing is UTC+8
  const d = new Date();
  const utc = d.getTime() + (d.getTimezoneOffset() * 60000);
  const beijingTime = new Date(utc + (3600000 * 8));
  
  const timeStr = beijingTime.toISOString().replace('T', ' ').replace('Z', '');
  const formattedLog = {
    time: timeStr,
    level: level.toUpperCase(), // 'INFO', 'WARN', 'ERROR', 'SUCCESS'
    message: message
  };
  
  logs.push(formattedLog);
  if (logs.length > maxLogs) logs.shift();
  
  // Console logging
  const colors = {
    INFO: '\x1b[36m',    // Cyan
    WARN: '\x1b[33m',    // Yellow
    ERROR: '\x1b[31m',   // Red
    SUCCESS: '\x1b[32m'  // Green
  };
  const resetColor = '\x1b[0m';
  const color = colors[formattedLog.level] || '';
  console.log(`[${timeStr}] [${color}${formattedLog.level}${resetColor}] ${message}`);
  
  if (global.logCallback) {
    global.logCallback(formattedLog);
  }
}

// Send Telegram alerts
async function sendTelegramAlert(text) {
  const settings = database.getSettings();
  if (!settings.telegramEnabled || !settings.telegramToken || !settings.telegramChatId) {
    return;
  }
  const url = `https://api.telegram.org/bot${settings.telegramToken}/sendMessage`;
  try {
    await axios.post(url, {
      chat_id: settings.telegramChatId,
      text: `🤖 【套利机器人告警】\n\n${text}\n\n[北京时间]: ${new Date(Date.now() + 8 * 3600000).toISOString().replace('T', ' ').replace('Z', '')}`,
      parse_mode: 'HTML'
    }, { timeout: 5000 });
  } catch (err) {
    console.error('Failed to send Telegram alert:', err.message);
  }
}

// Test Telegram Bot settings
async function testTelegram(token, chatId) {
  const url = `https://api.telegram.org/bot${token}/sendMessage`;
  const start = Date.now();
  try {
    const res = await axios.post(url, {
      chat_id: chatId,
      text: `🎉 套利机器人 Telegram 推送测试成功！\n测试延迟: ${Date.now() - start}ms`
    }, { timeout: 5000 });
    return { success: res.data.ok, message: 'Message sent successfully' };
  } catch (err) {
    return { success: false, error: err.response?.data?.description || err.message };
  }
}

// Lightweight latency check for any WebSocket URL
function testApiUrl(url) {
  return new Promise((resolve) => {
    const start = Date.now();
    let ws;
    try {
      ws = new WebSocket(url);
    } catch (e) {
      return resolve({ success: false, error: e.message });
    }
    
    let settled = false;
    ws.on('open', () => {
      ws.send(JSON.stringify({
        id: 1,
        jsonrpc: "2.0",
        method: "system_health",
        params: []
      }));
    });
    
    ws.on('message', () => {
      if (settled) return;
      settled = true;
      const latency = Date.now() - start;
      ws.close();
      resolve({ success: true, latency });
    });
    
    ws.on('error', (err) => {
      if (settled) return;
      settled = true;
      try { ws.close(); } catch (e) {}
      resolve({ success: false, error: err.message });
    });
    
    setTimeout(() => {
      if (settled) return;
      settled = true;
      try { ws.close(); } catch (e) {}
      resolve({ success: false, error: 'Connection Timeout' });
    }, 3000);
  });
}

// Fetch balances and nonces for all wallets
async function refreshWalletState(address) {
  if (!api || !api.isConnected) return;
  try {
    const account = await api.query.system.account(address);
    const freePlanck = BigInt(account.data.free.toString());
    const freeTao = Number(freePlanck) / 1e9;
    
    const nonce = await api.rpc.system.accountNextIndex(address);
    const nextNonce = Number(nonce.toString());
    
    balanceByAddress.set(address, { freeTao, updatedAt: new Date(Date.now() + 8 * 3600000).toISOString() });
    nextNonceByAddress.set(address, nextNonce);
  } catch (e) {
    log('WARN', `刷新钱包 ${address.slice(-6)} 状态失败: ${e.message}`);
  }
}

async function refreshAllWallets() {
  const activeWallets = getWalletsStatus();
  if (wallets.length === 0 || !api || !api.isConnected) return activeWallets;
  
  log('INFO', '正在通过 Batch Queries 批量更新钱包余额与 Nonce...');
  try {
    const addresses = wallets.map(w => w.pair.address);
    
    // Batch query account states
    const accounts = await api.query.system.account.multi(addresses);
    
    // Batch query nonces concurrently
    const noncePromises = addresses.map(addr => api.rpc.system.accountNextIndex(addr).catch(() => 0));
    const nonces = await Promise.all(noncePromises);
    
    for (let i = 0; i < wallets.length; i++) {
      const address = addresses[i];
      const account = accounts[i];
      const nextNonce = Number(nonces[i].toString());
      
      const freePlanck = BigInt(account.data.free.toString());
      const freeTao = Number(freePlanck) / 1e9;
      
      balanceByAddress.set(address, { freeTao, updatedAt: new Date(Date.now() + 8 * 3600000).toISOString() });
      nextNonceByAddress.set(address, nextNonce);
    }
  } catch (e) {
    log('WARN', `批量自愈刷新钱包状态失败，回退单包查询: ${e.message}`);
    const promises = wallets.map(w => refreshWalletState(w.pair.address));
    await Promise.allSettled(promises);
  }
  return getWalletsStatus();
}
// Reload wallets from database into memory dynamically
async function reloadWallets(actionContext = null) {
  await cryptoWaitReady();
  keyring = new Keyring({ type: 'sr25519' });
  
  if (actionContext) {
    log('SUCCESS', `[钱包管理] ${actionContext}`);
  }
  log('INFO', '正在重新加载数据库中的钱包至内存中...');
  const localWallets = database.getWallets(true); // Decrypted secrets
  const newWallets = [];
  
  for (const w of localWallets) {
    try {
      const pair = keyring.addFromUri(w.secret.trim());
      newWallets.push({
        name: w.name,
        pair: pair,
        enabled: true
      });
      log('INFO', `重新加载小号钱包: ${w.name} (${pair.address.slice(0, 8)}...${pair.address.slice(-6)})`);
    } catch (e) {
      log('ERROR', `重新加载钱包 ${w.name} 私钥失败: ${e.message}`);
    }
  }
  
  wallets = newWallets;
  await refreshAllWallets();
}

function getWalletsStatus() {
  const list = database.getWallets(false);
  return list.map(w => {
    const address = w.address || '';
    const balance = balanceByAddress.get(address);
    return {
      name: w.name,
      address: address,
      keyType: w.keyType,
      freeTao: balance ? balance.freeTao : null,
      updatedAt: balance ? balance.updatedAt : null
    };
  });
}

// Local Nonce management
function reserveNonce(address) {
  const nextNonce = nextNonceByAddress.get(address);
  if (nextNonce === undefined || isNaN(nextNonce)) return null;
  nextNonceByAddress.set(address, nextNonce + 1);
  return nextNonce;
}

// Send transaction helper
// Send transaction helper
async function sendTx(tx, pair, txTimeoutMs = 15000, tip = 0, meta = null) {
  return new Promise(async (resolve) => {
    let unsubscribe = null;
    let settled = false;
    const address = pair.address;
    
    // Check if we use a forced nonce (for RBF speedup) or reserve a new one
    const reservedNonce = (meta && meta.nonce !== undefined) ? meta.nonce : reserveNonce(address);
    
    const options = {};
    if (reservedNonce !== null) options.nonce = reservedNonce;
    if (tip > 0) options.tip = BigInt(Math.floor(tip * 1e9));

    const nonceKey = `${address}:${reservedNonce}`;
    const settings = database.getSettings();
    
    // If it's a new transaction (not a speedup itself) and RBF is enabled, track it
    if (meta && settings.replaceByFeeEnabled) {
      activePendingTxs.set(nonceKey, {
        nonce: reservedNonce,
        pair,
        tip,
        netuid: meta.netuid,
        hotkey: meta.hotkey,
        amountBigInt: meta.amountBigInt,
        slippageLimit: meta.slippageLimit,
        label: meta.label,
        sentAt: Date.now()
      });
    }

    const finish = (result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      activePendingTxs.delete(nonceKey);
      if (typeof unsubscribe === 'function') unsubscribe();
      resolve(result);
    };

    const timeout = setTimeout(() => {
      // Re-sync nonce if timeout occurs
      api.rpc.system.accountNextIndex(address)
        .then(n => nextNonceByAddress.set(address, Number(n.toString())))
        .catch(() => {});
      finish({ success: false, error: 'Transaction timeout' });
    }, txTimeoutMs);

    try {
      // Sign the transaction asynchronously
      await tx.signAsync(pair, options);
      const signedTxHex = tx.toHex();

      const callDetails = `${tx.method.section}.${tx.method.method}`;
      let callArgs = 'unknown';
      try {
        callArgs = JSON.stringify(tx.method.toHuman().args);
      } catch (argsErr) {}
      log('INFO', `[发送交易] 钱包【${pair.address.slice(-6)}】已签名并提交 ${callDetails}，参数: ${callArgs}，Nonce: ${reservedNonce}，Tip: ${tip} TAO`);

      // Parallel broadcast to all connected broadcast nodes
      broadcastSignedTx(signedTxHex);

      // Send pre-signed transaction via main node and subscribe
      tx.send(({ status, dispatchError }) => {
        if (status.isInBlock || status.isFinalized) {
          refreshWalletState(address).catch(() => {});
          if (dispatchError) {
            let errorInfo = dispatchError.toString();
            if (dispatchError.isModule) {
              const decoded = api.registry.findMetaError(dispatchError.asModule);
              errorInfo = `${decoded.section}.${decoded.name}: ${decoded.docs.join(' ')}`;
            }
            finish({ success: false, error: errorInfo });
          } else {
            finish({ success: true, hash: tx.hash.toHex() });
          }
        } else if (status.isError) {
          api.rpc.system.accountNextIndex(address)
            .then(n => nextNonceByAddress.set(address, Number(n.toString())))
            .catch(() => {});
          finish({ success: false, error: 'Chain transaction error' });
        }
      }).then((unsub) => {
        if (settled && typeof unsub === 'function') unsub();
        else unsubscribe = unsub;
      }).catch(error => {
        api.rpc.system.accountNextIndex(address)
          .then(n => nextNonceByAddress.set(address, Number(n.toString())))
          .catch(() => {});
        finish({ success: false, error: error.message });
      });
    } catch (err) {
      api.rpc.system.accountNextIndex(address)
        .then(n => nextNonceByAddress.set(address, Number(n.toString())))
        .catch(() => {});
      finish({ success: false, error: `Signing failed: ${err.message}` });
    }
  });
}

// Extrinsic parser to map values using metadata names
function parseExtrinsic(ext) {
  try {
    if (!ext || !ext.method) return null;
    
    const section = String(ext.method.section || '').trim();
    const callName = String(ext.method.method || '').trim();
    const signer = ext.signer ? ext.signer.toString() : 'unsigned';
    const txHash = ext.hash ? ext.hash.toHex() : 'unknown';
    const nonce = ext.nonce !== undefined && ext.nonce !== null
      ? Number(ext.nonce.toString())
      : null;

    // Decode extrinsic tip
    const tipBigInt = ext.tip ? BigInt(ext.tip.toString()) : 0n;
    const tipTao = Number(tipBigInt) / 1e9;

    // Decode arguments array
    const args = {};
    const argsArray = ext.method.args || [];
    for (let i = 0; i < argsArray.length; i++) {
      args[i] = argsArray[i];
    }
    
    // Map named arguments based on runtime metadata
    if (ext.method.meta && ext.method.meta.args) {
      for (let i = 0; i < ext.method.meta.args.length; i++) {
        const argMeta = ext.method.meta.args[i];
        const name = argMeta.name.toString();
        args[name] = argsArray[i];
      }
    }

    return {
      section,
      callName,
      signer,
      txHash,
      args,
      tipTao,
      nonce
    };
  } catch (err) {
    return null;
  }
}

// Compute next subnet to prune based on Yuma Consensus rules
async function getNextPruneCandidate(currentBlock) {
  let pruneNetuidVal = null;
  
  // 路径 A: 尝试 Runtime API 路径
  try {
    if (api.call.subnetInfoRuntimeApi && api.call.subnetInfoRuntimeApi.getSubnetToPrune) {
      pruneNetuidVal = await api.call.subnetInfoRuntimeApi.getSubnetToPrune();
    }
  } catch (err) {
    log('WARN', `通过 SubnetInfoRuntimeApi 运行时 API 查询失败: ${err.message}，将尝试其它路径...`);
  }

  // 路径 B: 原始底层 JSON-RPC 调用 (直接向 Web Provider 发送 raw 请求，最稳妥)
  if (pruneNetuidVal === null) {
    try {
      const providerInstance = api.rpc.provider || (api._rpcCore && api._rpcCore.provider);
      if (providerInstance && typeof providerInstance.send === 'function') {
        pruneNetuidVal = await providerInstance.send('subnetInfo_getSubnetToPrune', [null]);
      }
    } catch (err) {
      log('WARN', `通过 WsProvider 原始 JSON-RPC (subnetInfo_getSubnetToPrune) 查询失败: ${err.message}，将尝试其它路径...`);
    }
  }

  // 路径 C: 自定义 JSON-RPC 映射路径 (作为备用，传入 null 对齐)
  if (pruneNetuidVal === null) {
    try {
      if (api.rpc.subnetInfo && api.rpc.subnetInfo.getSubnetToPrune) {
        pruneNetuidVal = await api.rpc.subnetInfo.getSubnetToPrune(null);
      }
    } catch (err) {
      log('WARN', `通过 api.rpc.subnetInfo 自定义 RPC 映射查询失败: ${err.message}`);
    }
  }

  // 如果任何一条路径成功获取到值，尝试解析
  if (pruneNetuidVal !== undefined && pruneNetuidVal !== null) {
    try {
      let pruneNetuid;
      if (typeof pruneNetuidVal.isSome === 'boolean') {
        if (pruneNetuidVal.isSome) {
          pruneNetuid = Number(pruneNetuidVal.unwrap().toString());
        }
      } else {
        const valStr = pruneNetuidVal.toString();
        pruneNetuid = valStr.startsWith('0x') ? parseInt(valStr, 16) : Number(valStr);
      }
      if (pruneNetuid !== undefined && !isNaN(pruneNetuid)) {
        log('INFO', `[子网注销候选] 通过 SubnetInfo 接口直接查询成功，目标 netuid: #${pruneNetuid}`);
        return pruneNetuid;
      }
    } catch (decodeErr) {
      log('WARN', `解析待注销子网返回值失败: ${decodeErr.message}，将降级至批量多包计算`);
    }
  }

  // 降级使用批量多包并发查询进行本地 Yuma 规则计算
  try {
    const netuidKeys = await api.query.subtensorModule.networksAdded.keys();
    const activeNetuids = netuidKeys.map(({ args: [netuid] }) => netuid.toNumber());
    
    if (activeNetuids.length === 0) return null;

    // Batch query network parameters to minimize RPC roundtrips from 384 sequential queries to 3 batch queries
    const [registeredAtVals, immunityPeriodVals, emissionVals] = await Promise.all([
      api.query.subtensorModule.networkRegisteredAt.multi(activeNetuids),
      api.query.subtensorModule.immunityPeriod.multi(activeNetuids),
      api.query.subtensorModule.emission.multi(activeNetuids)
    ]);
    
    let bestCandidate = null;
    let lowestEmission = null;
    let earliestRegisteredAt = null;

    for (let i = 0; i < activeNetuids.length; i++) {
      const netuid = activeNetuids[i];
      const registeredAtVal = registeredAtVals[i];
      const immunityPeriodVal = immunityPeriodVals[i];
      const emissionVal = emissionVals[i];

      const registeredAt = Number(registeredAtVal?.toString() || 0);
      const immunityPeriod = Number(immunityPeriodVal?.toString() || 0);
      const emission = BigInt(emissionVal?.toString() || 0);

      // Check if past immunity period
      if (currentBlock - registeredAt >= immunityPeriod) {
        if (
          bestCandidate === null ||
          emission < lowestEmission ||
          (emission === lowestEmission && registeredAt < earliestRegisteredAt)
        ) {
          bestCandidate = netuid;
          lowestEmission = emission;
          earliestRegisteredAt = registeredAt;
        }
      }
    }
    return bestCandidate;
  } catch (err) {
    log('ERROR', `计算待注销子网候选失败: ${err.message}`);
    return null;
  }
}

// Fetch current Alpha price in RAO from runtime APIs
async function getSubnetPrice(netuid) {
  try {
    if (api.call.subnetInfoRuntimeApi && api.call.subnetInfoRuntimeApi.getSubnetInfoV2) {
      const info = await api.call.subnetInfoRuntimeApi.getSubnetInfoV2(netuid);
      if (info && info.isSome) {
        const unwrapped = info.unwrap();
        if (unwrapped.price) return BigInt(unwrapped.price.toString());
      }
    }
    if (api.call.subnetInfoRuntimeApi && api.call.subnetInfoRuntimeApi.getSubnetInfo) {
      const info = await api.call.subnetInfoRuntimeApi.getSubnetInfo(netuid);
      if (info && info.isSome) {
        const unwrapped = info.unwrap();
        if (unwrapped.price) return BigInt(unwrapped.price.toString());
      }
    }
  } catch (e) {
    log('WARN', `获取子网 #${netuid} 价格失败: ${e.message}`);
  }
  return null;
}

// Build addStake or addStakeLimit based on metadata compatibility
async function buildStakeTx(hotkey, netuid, amountBigInt, slippageLimit) {
  const hasLimitCall = typeof api.tx.subtensorModule.addStakeLimit === 'function';
  if (hasLimitCall && slippageLimit !== undefined) {
    const currentPrice = await getSubnetPrice(netuid);
    if (currentPrice !== null) {
      const settings = database.getSettings();
      const slippageMultiplier = 1.0 + parseFloat(slippageLimit);
      const limitPrice = BigInt(Math.floor(Number(currentPrice) * slippageMultiplier));
      const allowPartial = settings.allowPartialStaking !== false;
      
      log('INFO', `[限价保护] 启用 addStakeLimit -> 当前价格: ${(Number(currentPrice) / 1e9).toFixed(4)} TAO/Alpha, 设定限价: ${(Number(limitPrice) / 1e9).toFixed(4)} TAO/Alpha, 允许部分成交: ${allowPartial}`);
      return api.tx.subtensorModule.addStakeLimit(hotkey, netuid, amountBigInt, limitPrice, allowPartial);
    }
  }
  return api.tx.subtensorModule.addStake(hotkey, netuid, amountBigInt);
}

// Build removeStake or removeStakeLimit based on metadata compatibility
async function buildUnstakeTx(hotkey, netuid, amountBigInt, slippageLimit) {
  const hasLimitCall = typeof api.tx.subtensorModule.removeStakeLimit === 'function';
  if (hasLimitCall && slippageLimit !== undefined) {
    const currentPrice = await getSubnetPrice(netuid);
    if (currentPrice !== null) {
      const settings = database.getSettings();
      const slippageMultiplier = 1.0 - parseFloat(slippageLimit);
      const limitPrice = BigInt(Math.floor(Number(currentPrice) * slippageMultiplier));
      const allowPartial = settings.allowPartialStaking !== false;
      
      log('INFO', `[限价保护] 启用 removeStakeLimit -> 当前价格: ${(Number(currentPrice) / 1e9).toFixed(4)} TAO/Alpha, 最低限价: ${(Number(limitPrice) / 1e9).toFixed(4)} TAO/Alpha, 允许部分成交: ${allowPartial}`);
      return api.tx.subtensorModule.removeStakeLimit(hotkey, netuid, amountBigInt, limitPrice, allowPartial);
    }
  }
  return api.tx.subtensorModule.removeStake(hotkey, netuid, amountBigInt);
}

// Dynamic tip outbidding logic
function calculateDynamicTip(netuid, baseTip) {
  const settings = database.getSettings();
  if (!settings.dynamicTipEnabled) return baseTip;
  
  const maxMempoolTip = maxTipBySubnet.get(netuid) || 0;
  if (maxMempoolTip > 0) {
    const biddingTip = maxMempoolTip + (settings.dynamicTipMinDelta || 0.1);
    if (biddingTip > baseTip) {
      log('INFO', `[动态小费] 检测到交易池竞争，自动上浮小费: ${baseTip.toFixed(2)} TAO -> ${biddingTip.toFixed(2)} TAO (竞争对手最高小费: ${maxMempoolTip.toFixed(2)} TAO)`);
      return biddingTip;
    }
  }
  return baseTip;
}

// Strategy triggers and pending states
let pendingNewSubnet = null; // Staking Snipe target: { netuid, hotkey, detectedAt }
let pendingSandwichSell = null;

// Core extrinsic execution triggers
async function handlePendingExtrinsic(parsed) {
  const { callName, args, txHash, signer } = parsed;
  const settings = database.getSettings();
  const now = Date.now();

  const normalizedCall = callName.toLowerCase();

  // 1. registerNetwork / register_network -> 新子网 Staking 抢购
  if (/^register(_)?network$/i.test(normalizedCall)) {
    if (!settings.dashingEnabled) return true;
    
    try {
      const netuidKeys = await api.query.subtensorModule.networksAdded.keys();
      const numSubnets = netuidKeys.length;
      let targetNetuid = null;

      if (numSubnets >= 128) {
        targetNetuid = await getNextPruneCandidate(currentBlockHeight);
      } else {
        const activeNetuids = new Set(netuidKeys.map(({ args: [netuid] }) => netuid.toNumber()));
        let candidate = 1;
        while (activeNetuids.has(candidate)) {
          candidate++;
        }
        targetNetuid = candidate;
      }

      const targetHotkey = args.hotkey?.toString() || args[0]?.toString();
      
      if (targetNetuid !== null && targetHotkey) {
        let hotkeyExists = false;
        // 🔒 安全保护：Hotkey 拥有者关系校验 + Nonce/余额多级校验，防止误买旧子网
        try {
          const ownerQuery = api.query.subtensorModule.owner || api.query.subtensorModule.Owner;
          if (!ownerQuery || typeof ownerQuery.key !== 'function') {
            log('WARN', `[新子网打新] 拦截抢购：无法获取 subtensorModule.owner 查询接口，保守拦截以防误买。`);
            return true;
          }

          const ownerKey = ownerQuery.key(targetHotkey);
          const rawOwner = await api.rpc.state.getStorage(ownerKey);
          hotkeyExists = rawOwner !== null && rawOwner !== undefined && !rawOwner.isEmpty;

          if (hotkeyExists) {
            const ownerVal = await ownerQuery(targetHotkey);
            const ownerAddress = ownerVal.toString();

            // 情况 B：Hotkey 已存在，但 owner 不等于当前注册人 signer (注册必定失败)
            if (ownerAddress !== signer) {
              log('WARN', `[新子网打新] 过滤/拦截无效注册交易：Hotkey ${targetHotkey} 属于 ${ownerAddress}，并非当前注册人 ${signer}`);
              return true; // 拦截，跳过抢购
            }

            // 情况 C：Hotkey 已存在，且 owner == signer (存在旧子网冲突风险，启动严格校验)
            log('INFO', `[新子网打新] 检测到 Hotkey ${targetHotkey} 全局已存在且归属正确。启动 Nonce 和链上余额双重安全校验...`);

            // ①. 校验 Nonce 连续性 (必须 is 当前 nextNonce)
            const regAccount = await api.query.system.account(signer);
            const nextNonce = Number(regAccount.nonce.toString());
            if (parsed.nonce !== null && parsed.nonce !== nextNonce) {
              log('WARN', `[新子网打新] 拦截未来 Nonce 交易: Signer: ${signer}, 交易 Nonce: ${parsed.nonce}, 链上 Next Nonce: ${nextNonce}`);
              return true; // 拦截
            }

            // ②. 校验链上当前可用余额是否足够支付 Lock Cost
            const providerInstance = api.rpc.provider || (api._rpcCore && api._rpcCore.provider);
            if (!providerInstance || typeof providerInstance.send !== 'function') {
              log('WARN', `[新子网打新] 拦截抢购：RPC Provider 接口不可用，无法查询 Lock Cost。`);
              return true; // 拦截
            }

            const lockCostVal = await providerInstance.send('subnetInfo_getLockCost', [null]);
            if (!lockCostVal) {
              log('WARN', `[新子网打新] 拦截抢购：查询 subnetInfo_getLockCost 返回空值。`);
              return true; // 拦截
            }

            const lockCostRao = BigInt(lockCostVal.toString());
            const regFreeBalance = BigInt(regAccount.data.free.toString());

            if (regFreeBalance < lockCostRao) {
              log('WARN', `[新子网打新] 拦截当前可用余额不足交易: Signer: ${signer}, 链上余额: ${(Number(regFreeBalance) / 1e9).toFixed(2)} TAO, 需锁仓: ${(Number(lockCostRao) / 1e9).toFixed(2)} TAO`);
              return true; // 拦截
            }
          } else {
            // 情况 A：Hotkey 全局不存在
            // 提前 addStake 即使排在最前面，也会因为 HotKeyAccountNotExists 失败，无误买旧子网的风险，放行极速打新！
            log('INFO', `[新子网打新] Hotkey ${targetHotkey} 全局不存在，确认无误买风险，放行极速打新！`);
          }
        } catch (checkErr) {
          log('WARN', `[新子网打新] 执行多级安全校验时发生异常: ${checkErr.message}，已安全拦截抢购。`);
          return true; // 发生异常时，安全拦截，跳过抢购
        }

        const actionKey = `dashing:${targetNetuid}`;
        if (seenActions.has(actionKey)) return true;
        seenActions.set(actionKey, now);

        // 无论是全新还是已存热键，都在内存中登记 pendingNewSubnet，确保兜底生效
        pendingNewSubnet = {
          netuid: targetNetuid,
          hotkey: targetHotkey,
          detectedAt: now
        };

        if (hotkeyExists) {
          log('WARN', `[新子网打新] 检测到 Hotkey ${targetHotkey} 全局已存在。为防止同名回收或跨子网混淆风险，跳过 Mempool 立即开火，登记 Memory-Fallback，等待下个区块头后再补打，降低排在注册交易前面的风险。`);
          return true; // 结束 Mempool 触发
        }

        log('INFO', `[新子网打新] 扫到他人提交注册交易。预测 netuid #${targetNetuid}，提取到新子网目标 hotkey: ${targetHotkey}。且该 Hotkey 全局不存在，确认无误买风险，立即启动极速 Mempool Staking 抢单！`);
        
        // 立即执行极速抢单，并在后台进行重试与并发
        executeStakingSniping(targetNetuid, targetHotkey, 'Mempool').catch(e => {
          log('ERROR', `[新子网打新] 触发极速抢购失败: ${e.message}`);
        });

        return true;
      } else {
        log('WARN', `[新子网打新] 扫到注册网络交易，但无法解析目标 netuid (${targetNetuid}) 或 hotkey (${targetHotkey})`);
        return false;
      }
    } catch (e) {
      log('ERROR', `处理新子网打新判断错误: ${e.message}`);
      return false;
    }
  }

  // 2. setSubnetIdentity / set_subnet_identity -> 子网改名抢跑
  if (/^set(_)?subnet(_)?identity$/i.test(normalizedCall)) {
    if (!settings.renameEnabled) return true;
    try {
      const netuid = Number(args.netuid?.toString() || args[0]?.toString());
      const nameRaw = args.subnet_name || args.name || args[1];
      let cleanName = '';

      if (nameRaw) {
        const human = nameRaw.toHuman();
        if (typeof human === 'string') {
          cleanName = human.startsWith('0x') ? Buffer.from(human.slice(2), 'hex').toString('utf8').trim() : human.trim();
        } else if (Array.isArray(human)) {
          cleanName = String.fromCharCode(...human).trim();
        }
      }

      if (netuid && cleanName) {
        // 🔒 安全保护 1：如果新名字是占位符（如 Subnet X、unknown、none 或空值），判定为身份清空或重置，不进行抢跑。
        const defaultPattern = new RegExp(`^Subnet\\s*(${netuid}|x)$`, 'i');
        const isNewPlaceholder = !cleanName ||
                                 cleanName.trim() === '' ||
                                 defaultPattern.test(cleanName) ||
                                 /^(unknown|unknow|none|null|undefined)$/i.test(cleanName) ||
                                 /^subnet\s*\d+$/i.test(cleanName);
        if (isNewPlaceholder) {
          log('INFO', `[改名抢跑] 检测到子网 #${netuid} 新拟改名字 "${cleanName}" 为占位符或空值，判定为身份清空，跳过改名抢跑。`);
          return true;
        }

        // 🔒 安全保护 2：如果是全新创建的子网首次起名，跳过改名抢跑以避免与策略 1 重复买入。
        // 我们通过查询内存缓存获取其注册区块年龄（0ms 延迟，不拖慢抢跑速度）
        const registeredAt = subnetRegisteredAtCache.get(netuid) || 0;
        if (registeredAt > 0 && currentBlockHeight - registeredAt < 100) {
          log('INFO', `[改名抢跑] 子网 #${netuid} 为近 ${currentBlockHeight - registeredAt} 个区块内刚创建的新子网（内存判定）。跳过改名抢跑（由策略 1 负责 Staking 抢购），防止重复买入。`);
          return true;
        }

        const actionKey = `rename:${netuid}:${cleanName}`;
        if (seenActions.has(actionKey)) return true;

        log('INFO', `[改名抢跑] 扫到子网 #${netuid} 提交改名交易 -> "${cleanName}" (Hash: ${txHash})`);
        
        const targetHotkey = await resolveHotkey(netuid);
        if (targetHotkey) {
          seenActions.set(actionKey, now);
          executeArbitrageStake(netuid, targetHotkey, settings.renameAmount, settings.renameTip, '改名抢跑', settings.renameSlippageLimit);
          return true;
        } else {
          log('WARN', `[改名抢跑] 无法为子网 #${netuid} 解析到有效 hotkey，取消抢跑。`);
          return false;
        }
      } else {
        log('WARN', `[改名抢跑] 无法解析 netuid (${netuid}) 或 cleanName (${cleanName})`);
        return false;
      }
    } catch (e) {
      log('ERROR', `处理改名抢跑判断错误: ${e.message}`);
      return false;
    }
  }

  // 3. announceColdkeySwap -> 冷键交换声明抢跑（仅匹配 announceColdkeySwap / announce_coldkey_swap）
  if (/^(announceColdkeySwap|announce_coldkey_swap)$/i.test(normalizedCall)) {
    if (!settings.swapEnabled) return true;
    try {
      const oldColdkey = signer;
      if (oldColdkey && oldColdkey !== 'unsigned' && oldColdkey !== 'unknown') {
        log('INFO', `[冷键交换抢跑] 扫到交换冷键声明 -> ${callName} (Old Coldkey: ${oldColdkey})`);

        let matched = false;
        let anyHotkeyResolveFailed = false;

        for (const [netuid, owner] of subnetOwnersCache.entries()) {
          try {
            if (owner === oldColdkey) {
              matched = true;
              const actionKey = `swap:${netuid}:${oldColdkey}`;
              if (seenActions.has(actionKey)) continue;

              log('INFO', `[冷键交换抢跑] 匹配到目标受控子网 #${netuid}，立即执行抢跑！`);
              const targetHotkey = await resolveHotkey(netuid);
              if (targetHotkey) {
                seenActions.set(actionKey, now);
                executeArbitrageStake(netuid, targetHotkey, settings.swapAmount, settings.swapTip, '冷键交换抢跑', settings.swapSlippageLimit);
              } else {
                log('WARN', `[冷键交换抢跑] 无法为子网 #${netuid} 解析到有效 hotkey，取消抢跑。`);
                anyHotkeyResolveFailed = true;
              }
            }
          } catch (e) {
            anyHotkeyResolveFailed = true;
          }
        }

        if (matched) {
          return !anyHotkeyResolveFailed;
        } else {
          if (subnetOwnersCache.size === 0) {
            log('WARN', `[冷键交换抢跑] 活跃子网 Owner 缓存为空，无法判断是否需要抢跑，跳过本次处理并等待重试...`);
            return false;
          }
          return true;
        }
      } else {
        log('WARN', `[冷键交换抢跑] 扫到交换 coldkey 交易，但无法解析 oldColdkey`);
        return false;
      }
    } catch (e) {
      log('ERROR', `处理冷键交换抢跑判断错误: ${e.message}`);
      return false;
    }
  }

  // 4. addStake / addStakeLimit -> 大额买入三明治套利（彻底排除 swapStake 干扰）
  if (/^(add(_)?stake|add(_)?stake(_)?limit)$/i.test(normalizedCall)) {
    if (!settings.sandwichEnabled) return true;
    try {
      let netuid = null;
      let hotkey = null;
      let amountRao = 0n;
      let isBuy = false;

      if (/^add(_)?stake/i.test(normalizedCall)) {
        hotkey = args.hotkey?.toString() || args[0]?.toString();
        netuid = Number(args.netuid?.toString() || args[1]?.toString());
        amountRao = BigInt(
          args.amountStaked?.toString() || 
          args.amount_staked?.toString() || 
          args.stake_to_be_added?.toString() || 
          args.amount?.toString() || 
          args[2]?.toString() || 
          '0'
        );
        isBuy = true;
      }

      if (isBuy && netuid !== null && hotkey) {
        if (!Number.isFinite(netuid) || netuid <= 0) return true; // 严格过滤无效子网和子网 0
        const amountTao = Number(amountRao) / 1e9;
        if (amountTao >= settings.sandwichThreshold) {
          const actionKey = `sandwich:${txHash}`;
          if (seenActions.has(actionKey)) return true;

          // 核心验证：过滤虚假/无效交易（无 nonce/signer 则保守跳过）
          if (!signer || signer === 'unsigned' || parsed.nonce === null) {
            log('INFO', `[三明治套利] 过滤无法解析 signer 或 nonce 的交易: Hash: ${txHash}, Signer: ${signer}, Nonce: ${parsed.nonce}`);
            return true;
          }

          try {
            const accountInfo = await api.query.system.account(signer);
            const nextNonce = accountInfo.nonce.toNumber();
            
            // 严格要求 nonce 必须等于 nextNonce，过滤过期或未来 nonce 交易，确保紧挨着夹人
            if (parsed.nonce !== nextNonce) {
              log('INFO', `[三明治套利] 过滤 Nonce 不匹配 the交易 (未来或过期): Hash: ${txHash}, Signer: ${signer}, TxNonce: ${parsed.nonce}, ChainNonce: ${nextNonce}`);
              return true;
            }
            
            const freeBalance = BigInt(accountInfo.data.free.toString());
            if (freeBalance < amountRao) {
              log('WARN', `[三明治套利] 过滤余额不足的虚假买入交易: Hash: ${txHash}, Signer: ${signer}, 余额: ${(Number(freeBalance) / 1e9).toFixed(2)} TAO, 需质押: ${(Number(amountRao) / 1e9).toFixed(2)} TAO`);
              return true;
            }
          } catch (err) {
            log('WARN', `[三明治套利] 验证交易有效性时发生异常: ${err.message}，放弃本次套利。`);
            return true; // 验证失败保守跳过，确保安全
          }

          seenActions.set(actionKey, now);

          log('WARN', `[三明治套利] 扫到大额买入交易 (金额: ${amountTao.toFixed(2)} TAO, 子网 #${netuid}, 目标 Hotkey: ${hotkey}, 夹人地址: ${signer}) (Hash: ${txHash})！`);
          sendTelegramAlert(`🚨 [三明治套利触发]\n检测到大额买入交易！\n金额: ${amountTao.toFixed(2)} TAO\n子网: #${netuid}\n目标 Hotkey: ${hotkey}\n发送者: ${signer}\n正在执行前置抢跑买入...`);

          // Calculate dynamic slippage
          let calculatedSlippage;
          try {
            calculatedSlippage = await calculateDynamicSlippage(netuid, amountTao);
          } catch (e) {
            log('WARN', `[三明治套利] 计算动态滑点失败: ${e.message}。使用默认滑点 0.05`);
            calculatedSlippage = 0.05;
          }
          
          if (settings.dynamicSlippageEnabled) {
            const safetyFactor = settings.dynamicSlippageSafetyFactor || 0.7;
            const expectedRise = calculatedSlippage / safetyFactor;
            if (expectedRise < 0.015) {
              log('INFO', `[三明治套利] 子网 #${netuid} 预测价格涨幅 ${(expectedRise * 100).toFixed(2)}% 过低，无法覆盖套利小费，放弃本次套利。`);
              return true;
            }
          }

          const buySuccess = await executeSandwichBuy(netuid, hotkey, settings.sandwichAmount, settings.sandwichTip, calculatedSlippage);
          if (buySuccess) {
            if (settings.sandwichAutoSell) {
              log('INFO', `[三明治套利] 成功执行前置买入，已登记后置卖出，将在下一个区块头确认时自动发起售出...`);
              pendingSandwichSell = {
                netuid,
                hotkey,
                amount: settings.sandwichAmount,
                tip: settings.sandwichSellTip
              };
            }
            return true;
          } else {
            log('WARN', `[三明治套利] 执行前置买入失败，本次套利取消。`);
            return false;
          }
        } else {
          return true;
        }
      } else {
        return false;
      }
    } catch (e) {
      log('ERROR', `处理大额买入抢跑判断错误: ${e.message}`);
      return false;
    }
  }

  return true;
}

// Block-Fallback scanner for missed mempool transactions (Strategy 2 & 3)
async function detectEventsInBlock(blockHash, blockNumber) {
  const settings = database.getSettings();
  if (!settings.renameEnabled && !settings.swapEnabled && !settings.dashingEnabled) return;

  // 兜底防空保护：确保 Owner 缓存已经同步就绪
  if (settings.swapEnabled && subnetOwnersCache.size === 0) {
    await refreshSubnetOwnersCache();
  }

  // 获取区块的所有事件并解析输出日志
  let allRecords = [];
  try {
    allRecords = await api.query.system.events.at(blockHash);
  } catch (err) {
    // 忽略获取事件异常
  }

  if (allRecords && allRecords.length > 0) {
    allRecords.forEach(({ event, phase }) => {
      if (!phase.isApplyExtrinsic) return;
      const extrinsicIndex = phase.asApplyExtrinsic.toNumber();
      const section = event.section;
      const method = event.method;
      const data = event.data.toHuman();

      // 1. NetworkAdded (新子网注册成功)
      if (section === 'subtensorModule' && method === 'NetworkAdded') {
        const netuid = data[0];
        const logMsg = `[新子网打新] 目标子网 #${netuid} 已于区块 #${blockNumber} 第 ${extrinsicIndex} 笔交易正式注册成功！`;
        log('SUCCESS', logMsg);
        sendTelegramAlert(`🎉 ${logMsg}`).catch(() => {});
      }

      // 2. SubnetIdentitySet (子网改名成功)
      if (section === 'subtensorModule' && method === 'SubnetIdentitySet') {
        const netuid = data[0];
        const logMsg = `[改名抢跑] 目标子网 #${netuid} 已于区块 #${blockNumber} 第 ${extrinsicIndex} 笔交易正式改名成功！`;
        log('SUCCESS', logMsg);
        sendTelegramAlert(`🎉 ${logMsg}`).catch(() => {});
      }

      // 3. ColdkeySwapAnnounced (冷键交换声明成功)
      if (section === 'subtensorModule' && method === 'ColdkeySwapAnnounced') {
        const coldkey = data[0];
        const swapColdkey = data[1];
        const logMsg = `[冷键交换] 钱包 ${coldkey} 已于区块 #${blockNumber} 第 ${extrinsicIndex} 笔交易正式发起冷键交换声明 -> ${swapColdkey}！`;
        log('SUCCESS', logMsg);
        sendTelegramAlert(`🎉 ${logMsg}`).catch(() => {});
      }

      // 4. StakeAdded (质押成功 - 检查是否是我们的钱包)
      if (section === 'subtensorModule' && method === 'StakeAdded') {
        const coldkey = data[0];
        const hotkey = data[1];
        const amountRao = data[2];
        const netuid = data[4];

        const w = wallets.find(x => x.pair && x.pair.address === coldkey);
        if (w) {
          const amountTao = (Number(amountRao.toString().replace(/,/g, '')) / 1e9).toFixed(2);
          log('SUCCESS', `[打新/抢跑成功] 我们的钱包【${w.name}】已于区块 #${blockNumber} 第 ${extrinsicIndex} 笔交易成功在子网 #${netuid} 质押！金额: ${amountTao} TAO (Hotkey: ${hotkey})`);
          
          sendTelegramAlert(`🔔 [打新/抢跑 链上最终确认]\n我们的钱包: 【${w.name}】\n已于区块: #${blockNumber} 第 ${extrinsicIndex} 笔交易最终质押成功！\n金额: ${amountTao} TAO\n子网: #${netuid}\nHotkey: ${hotkey}`).catch(() => {});
        }
      }
    });
  }

  const block = await api.rpc.chain.getBlock(blockHash);
  const extrinsics = block?.block?.extrinsics;
  if (!extrinsics || extrinsics.length === 0) return;

  const now = Date.now();
  for (const ext of extrinsics) {
    if (!ext || !ext.method) continue;

    const sec = String(ext.method.section || '').trim();
    const meth = String(ext.method.method || '').trim();

    // Cheap string checks first to save CPU before parsing
    const isRename = settings.renameEnabled && 
      /^subtensor(Module)?$/i.test(sec) && 
      /^(setSubnetIdentity|set_subnet_identity)$/i.test(meth);

    const isSwap = settings.swapEnabled && 
      /^subtensor(Module)?$/i.test(sec) && 
      /^(announceColdkeySwap|announce_coldkey_swap)$/i.test(meth);

    if (!isRename && !isSwap) continue;

    try {
      const parsed = parseExtrinsic(ext);
      if (!parsed) continue;

      // seenHashes 防重过滤
      const entry = seenHashes.get(parsed.txHash);
      if (entry && entry.handled) continue;

      if (isRename) {
        log('INFO', `[区块兜底] 在区块 #${blockNumber} 中补扫到漏掉的改名交易 (Hash: ${parsed.txHash})`);
        const handled = await handlePendingExtrinsic(parsed);
        seenHashes.set(parsed.txHash, {
          timestamp: now,
          netuid: null,
          tipTao: parsed.tipTao,
          isRegisterNetwork: false,
          handled: !!handled
        });
      } else if (isSwap) {
        log('INFO', `[区块兜底] 在区块 #${blockNumber} 中补扫到漏掉的冷键交换声明交易 (Hash: ${parsed.txHash})`);
        const handled = await handlePendingExtrinsic(parsed);
        seenHashes.set(parsed.txHash, {
          timestamp: now,
          netuid: null,
          tipTao: parsed.tipTao,
          isRegisterNetwork: false,
          handled: !!handled
        });
      }
    } catch (err) {
      // 单个 extrinsic 的执行/解析异常，不影响其他 extrinsic 运行
    }
  }
}

// Resolve a valid hotkey for a given subnet using multi-tiered fallback
async function resolveHotkey(netuid, force = false) {
  if (!force) {
    const cached = subnetHotkeysCache.get(netuid);
    if (cached && cached.length >= 47) return cached;
  }

  try {
    const ownerHotkeyObj = await api.query.subtensorModule.subnetOwnerHotkey(netuid);
    if (ownerHotkeyObj && !ownerHotkeyObj.isEmpty) {
      const hk = ownerHotkeyObj.toString();
      if (hk && hk.length >= 47) {
        subnetHotkeysCache.set(netuid, hk);
        return hk;
      }
    }
  } catch (e) {
    log('WARN', `[解析Hotkey] 通过 subnetOwnerHotkey 查询子网 #${netuid} 失败: ${e.message}`);
  }

  try {
    const uids = [0, 1, 2, 3, 4, 5];
    const keys = await api.query.subtensorModule.keys.multi(uids.map(uid => [netuid, uid]));
    for (const keyObj of keys) {
      if (keyObj && !keyObj.isEmpty) {
        const hk = keyObj.toString();
        if (hk && hk.length >= 47) {
          subnetHotkeysCache.set(netuid, hk);
          return hk;
        }
      }
    }
  } catch (e) {
    log('WARN', `[解析Hotkey] 通过 keys.multi 查询子网 #${netuid} 失败: ${e.message}`);
  }
  
  try {
    const owner = await api.query.subtensorModule.subnetOwner(netuid);
    if (owner && !owner.isEmpty) {
      const ownerStr = owner.toString();
      if (ownerStr && ownerStr.length >= 47) {
        subnetHotkeysCache.set(netuid, ownerStr);
        return ownerStr;
      }
    }
  } catch (e) {
    log('WARN', `[解析Hotkey] 通过 subnetOwner 查询子网 #${netuid} 失败: ${e.message}`);
  }
  
  return null;
}

// Execute normal staking arbitrage
// Execute normal staking arbitrage
async function executeArbitrageStake(netuid, hotkey, amountTao, tip, label, slippageLimit) {
  const settings = database.getSettings();
  const activeWallets = wallets.filter(w => w.enabled !== false);
  if (activeWallets.length === 0) {
    log('WARN', `[${label}] 触发抢跑，但没有加载启用任何小号钱包！`);
    return;
  }

  // 根据策略类型提取对应的配置参数，默认回退安全值
  let burstCount = 1;
  let retries = 1;
  let interval = 1000;
  let timeoutMs = 30000;
  let timeoutRetries = 0;

  if (label === '改名抢跑') {
    burstCount = Math.max(1, settings.renameBurstCount || 1);
    retries = Math.max(1, settings.renameRetries || 1);
    interval = Math.max(50, settings.renameIntervalMs || 1000);
    timeoutMs = Math.max(1000, settings.renameTimeoutMs || 30000);
    timeoutRetries = Math.max(0, settings.renameTimeoutRetries || 0);
  } else if (label === '冷键交换抢跑') {
    burstCount = Math.max(1, settings.swapBurstCount || 1);
    retries = Math.max(1, settings.swapRetries || 1);
    interval = Math.max(50, settings.swapIntervalMs || 1000);
    timeoutMs = Math.max(1000, settings.swapTimeoutMs || 30000);
    timeoutRetries = Math.max(0, settings.swapTimeoutRetries || 0);
  }

  let strategyName = 'rename';
  if (label === '冷键交换抢跑') {
    strategyName = 'coldkey-swap';
  }

  const successKey = `${label}:${netuid}:${hotkey}`;
  const lockKey = `lock:${label}:${netuid}`;

  // 1. 防重复运行锁
  if (activeSnipesByNetuid.has(lockKey)) {
    log('INFO', `[${label}] 子网 #${netuid} 抢跑循环已经在运行中，跳过重复触发。`);
    return;
  }

  // 2. 24小时冷却时间校验（持久化，不受重启影响，不管成功与否）
  const cooldownKey = `${strategyName}:${netuid}`;
  const cooldown = database.getCooldown(cooldownKey);
  let shouldWriteCooldown = false;

  if (cooldown) {
    const elapsed = Date.now() - cooldown.firstTriggeredAt;
    if (elapsed < 24 * 60 * 60 * 1000) {
      if (Math.abs(currentBlockHeight - cooldown.block) <= 10) {
        shouldWriteCooldown = false; // 同一次事件兜底，不刷新冷却
      } else {
        log('INFO', `[${label}] 检测到子网 #${netuid} 上次触发在 24 小时冷却时间内 (上次区块: #${cooldown.block}, 当前区块: #${currentBlockHeight})，且已超过防抖窗口，跳过重复触发。`);
        return;
      }
    } else {
      // 冷却已过，清理可能残留的内存成功状态
      dashingSuccessByNetuid.delete(successKey);
      shouldWriteCooldown = true;
    }
  } else {
    dashingSuccessByNetuid.delete(successKey);
    shouldWriteCooldown = true;
  }

  // 3. 内存成功状态校验（用于单次扫描中成功后提前退出）
  if (dashingSuccessByNetuid.get(successKey) === true) {
    log('INFO', `[${label}] 检测到子网 #${netuid} 之前已抢跑成功，跳过执行。`);
    return;
  }

  // 写入冷却时间（不论成功与否均冷却 24 小时，同一次防抖窗口内不重复刷新）
  if (shouldWriteCooldown) {
    const ok = database.setCooldown(cooldownKey, {
      strategy: strategyName,
      netuid: netuid,
      block: currentBlockHeight,
      hotkey: hotkey
    });
    if (!ok) {
      log('WARN', `[${label}] 冷却状态写入失败: key = ${cooldownKey}`);
    }
  }

  // 加锁并初始化成功状态
  activeSnipesByNetuid.add(lockKey);
  if (dashingSuccessByNetuid.get(successKey) === undefined) {
    dashingSuccessByNetuid.set(successKey, false);
  }

  const targetTip = calculateDynamicTip(netuid, tip);
  log('INFO', `[${label}] 启动抢跑机制 -> 目标子网 #${netuid}, 目标 Hotkey: ${hotkey}, 单轮并发数: ${burstCount}, 最大扫射轮数: ${retries}轮, 扫射间隔: ${interval}ms`);
  sendTelegramAlert(`🚀 [${label} 触发]\n子网: #${netuid}\n目标 Hotkey: ${hotkey}\n单轮并发数: ${burstCount}\n最大扫射轮数: ${retries}轮\n扫射间隔: ${interval}ms`);

  const amountBigInt = BigInt(Math.floor(amountTao * 1e9));
  const txPromises = [];

  try {
    for (let attempt = 0; attempt < retries; attempt++) {
      if (attempt > 0 && dashingSuccessByNetuid.get(successKey)) {
        log('INFO', `[${label}] 检测到已有并发购买交易成功上链，自动终止后续的第 ${attempt + 1}/${retries} 轮扫射。`);
        break;
      }

      log('INFO', `[${label}] 开始执行第 ${attempt + 1}/${retries} 轮扫射尝试...`);

      for (const w of activeWallets) {
        for (let i = 0; i < burstCount; i++) {
          try {
            const tx = await buildStakeTx(hotkey, netuid, amountBigInt, slippageLimit);
            log('INFO', `[${label}] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1}/${burstCount} 笔购买交易发起...`);

            const p = sendTx(tx, w.pair, timeoutMs, targetTip, {
              netuid,
              hotkey,
              amountBigInt,
              slippageLimit,
              label: `${label}-轮次${attempt + 1}-并发#${i + 1}`
            }).then(res => {
              if (res.success) {
                log('SUCCESS', `[${label} 成功] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔购买成功！Hash: ${res.hash}`);
                dashingSuccessByNetuid.set(successKey, true);
                sendTelegramAlert(`✅ [${label} 成功]\n钱包: ${w.name}\n子网: #${netuid}\n交易哈希: ${res.hash}`);
                return res;
              } else {
                log('ERROR', `[${label} 失败] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔交易失败: ${res.error}`);
                if (res.error && (res.error.includes('timeout') || res.error.includes('Timeout')) && timeoutRetries > 0) {
                  return executeTimeoutRetry(w, netuid, hotkey, 1, timeoutRetries, timeoutMs, amountTao, slippageLimit, tip, label);
                }
                return res;
              }
            });
            txPromises.push(p);
          } catch (e) {
            log('ERROR', `[${label}] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔交易抛出异常: ${e.message}`);
          }
        }
      }

      if (attempt < retries - 1) {
        await new Promise(resolve => setTimeout(resolve, interval));
      }
    }
  } finally {
    const unlock = () => {
      activeSnipesByNetuid.delete(lockKey);
    };

    if (txPromises.length > 0) {
      Promise.allSettled(txPromises).finally(unlock);
      setTimeout(unlock, 180000); // 3-minute safety release
    } else {
      setTimeout(unlock, Math.max(3000, interval));
    }
  }
}

// Helper to query Alpha balance from chain and log the raw value
async function queryAlphaBalance(hotkey, coldkey, netuid) {
  if (!api.query.subtensorModule.alpha) {
    log('WARN', `[余额查询] api.query.subtensorModule.alpha 不存在，尝试使用旧版本 stake 查询`);
    if (api.query.subtensorModule.stake) {
      const stakeVal = await api.query.subtensorModule.stake(hotkey, coldkey);
      log('INFO', `[余额查询] stake (旧版本) 查询结果 raw: ${stakeVal.toString()}`);
      return BigInt(stakeVal.toString());
    }
    return 0n;
  }
  
  const alphaVal = await api.query.subtensorModule.alpha(hotkey, coldkey, netuid);
  const alphaRao = BigInt(alphaVal.toString());
  log('INFO', `[余额查询] alpha 查询结果 raw (Rao): ${alphaRao.toString()} (Hotkey: ${hotkey.slice(-6)}, Coldkey: ${coldkey.slice(-6)}, Netuid: ${netuid})`);
  return alphaRao;
}

// Sandwich Buy (Frontrun)
async function executeSandwichBuy(netuid, hotkey, amountTao, tip, slippageLimit) {
  const settings = database.getSettings();
  const activeWallets = wallets.filter(w => w.enabled !== false);
  if (activeWallets.length === 0) return false;
  
  const w = activeWallets[0];
  const amountBigInt = BigInt(Math.floor(amountTao * 1e9));
  const targetTip = calculateDynamicTip(netuid, tip);
  
  try {
    const tx = await buildStakeTx(hotkey, netuid, amountBigInt, slippageLimit);
    log('INFO', `[三明治套利] 发起前置抢跑买入 -> 钱包: ${w.name}, 小费: ${targetTip} TAO`);
    const res = await sendTx(tx, w.pair, settings.sandwichTimeoutMs || 18000, targetTip, {
      netuid,
      hotkey,
      amountBigInt,
      slippageLimit,
      label: '三明治套利买入'
    });
    if (res.success) {
      log('SUCCESS', `[三明治套利] 前置买入成功！Hash: ${res.hash}`);
      return true;
    } else {
      log('ERROR', `[三明治套利] 前置买入失败: ${res.error}`);
      sendTelegramAlert(`❌ [三明治套利 失败]\n前置买入失败，终止套利动作。\n原因: ${res.error}`);
      return false;
    }
  } catch (e) {
    log('ERROR', `[三明治套利] 前置买入异常: ${e.message}`);
    return false;
  }
}

// Sandwich Sell (Backrun)
async function executeSandwichSell(netuid, hotkey, amountTao, tip) {
  const settings = database.getSettings();
  const activeWallets = wallets.filter(w => w.enabled !== false);
  if (activeWallets.length === 0) return;
  
  const w = activeWallets[0];
  const targetTip = calculateDynamicTip(netuid, tip);
  
  // Query actual Alpha stake balance of the wallet on the subnet
  let stakeRao = 0n;
  try {
    log('INFO', `[三明治套利] 正在查询钱包【${w.name}】在子网 #${netuid} 的实际 Alpha 质押余额...`);
    for (let attempt = 0; attempt < 5; attempt++) {
      stakeRao = await queryAlphaBalance(hotkey, w.pair.address, netuid);
      if (stakeRao > 0n) break;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  } catch (err) {
    log('WARN', `[三明治套利] 查询 stake 余额异常: ${err.message}`);
  }

  if (stakeRao === 0n) {
    log('ERROR', `[三明治套利] 未查询到实际 Alpha 质押余额，放弃盲目卖出以防损失！请手动查账！`);
    sendTelegramAlert(`❌ [三明治套利异常]\n前置买入已成功，但无法查询到实际质押余额，终止自动卖出！请立即手动查账处理！`);
    return;
  } else {
    log('INFO', `[三明治套利] 成功获取到实际 Alpha 质押余额: ${(Number(stakeRao) / 1e9).toFixed(4)} Alpha`);
  }
  
  try {
    const tx = await buildUnstakeTx(hotkey, netuid, stakeRao, settings.sandwichSlippageLimit);
    log('INFO', `[三明治套利] 发起后置卖出 -> 钱包: ${w.name}, 数量: ${(Number(stakeRao) / 1e9).toFixed(4)} Alpha, 小费: ${targetTip} TAO`);
    const res = await sendTx(tx, w.pair, settings.sandwichTimeoutMs || 18000, targetTip);
    if (res.success) {
      log('SUCCESS', `[三明治套利] 后置卖出回补成功！套利完成！Hash: ${res.hash}`);
      sendTelegramAlert(`🎉 [三明治套利成功]\n钱包: ${w.name}\n子网: #${netuid}\n卖出数量: ${(Number(stakeRao) / 1e9).toFixed(4)} Alpha\n卖出交易哈希: ${res.hash}`);
    } else {
      log('ERROR', `[三明治套利] 后置卖出失败: ${res.error}`);
      sendTelegramAlert(`❌ [三明治套利异常]\n前置买入已成功，但后置卖出失败！请手动查账！\n原因: ${res.error}`);
    }
  } catch (e) {
    log('ERROR', `[三明治套利] 后置卖出异常: ${e.message}`);
  }
}

// Helper for timeout retries
async function executeTimeoutRetry(w, netuid, targetHotkey, attemptNum, maxTimeoutRetries = null, customTimeoutMs = null, customAmount = null, customSlippageLimit = null, customTip = null, label = '新子网打新') {
  const settings = database.getSettings();
  const actualMaxRetries = maxTimeoutRetries !== null ? maxTimeoutRetries : (settings.dashingTimeoutRetries || 0);
  if (attemptNum > actualMaxRetries) return { success: false, error: 'Max timeout retries reached' };
  
  const successKey = `${label}:${netuid}:${targetHotkey}`;
  if (dashingSuccessByNetuid.get(successKey)) return { success: true };
  
  // Prevent duplicate concurrent timeout retries for the same wallet
  const key = `${label}:${netuid}:${targetHotkey}:${w.name}`;
  const currentActive = activeTimeoutRetryNumByWallet.get(key) || 0;
  if (attemptNum <= currentActive) return { success: false, error: 'Duplicate retry' };
  activeTimeoutRetryNumByWallet.set(key, attemptNum);
  
  log('WARN', `[${label}] 钱包【${w.name}】交易超时。触发第 ${attemptNum}/${actualMaxRetries} 次超时重试...`);
  
  // Wait 1 second before retrying to ensure the nonce query inside sendTx timeout handler completed
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  if (dashingSuccessByNetuid.get(successKey)) return { success: true };
  
  const actualAmount = customAmount !== null ? customAmount : settings.dashingAmount;
  const actualSlippageLimit = customSlippageLimit !== null ? customSlippageLimit : settings.dashingSlippageLimit;
  const actualTip = customTip !== null ? customTip : settings.dashingTip;
  const actualTimeoutMs = customTimeoutMs !== null ? customTimeoutMs : (settings.dashingTimeoutMs || 30000);
  
  try {
    const targetTip = calculateDynamicTip(netuid, actualTip);
    const amountBigInt = BigInt(Math.floor(actualAmount * 1e9));
    const tx = await buildStakeTx(targetHotkey, netuid, amountBigInt, actualSlippageLimit);
    
    const p = new Promise((resolve) => {
      sendTx(tx, w.pair, actualTimeoutMs, targetTip, {
        netuid,
        hotkey: targetHotkey,
        amountBigInt,
        slippageLimit: actualSlippageLimit,
        label: `${label}-超时重试#${attemptNum}`
      }).then(res => {
        if (res.success) {
          log('SUCCESS', `[${label}] 超时重试 #${attemptNum} - 钱包【${w.name}】购买成功！Hash: ${res.hash}`);
          dashingSuccessByNetuid.set(successKey, true);
          if (label === '新子网打新') {
            pendingNewSubnet = null; // 清除内存兜底，防重复触发
          }
          sendTelegramAlert(`✅ [${label} 超时重试成功]\n钱包: ${w.name}\n子网: #${netuid}\n重试次数: ${attemptNum}\n交易哈希: ${res.hash}`);
          resolve(res);
        } else {
          log('ERROR', `[${label}] 超时重试 #${attemptNum} - 钱包【${w.name}】交易失败: ${res.error}`);
          if (res.error && (res.error.includes('timeout') || res.error.includes('Timeout'))) {
            executeTimeoutRetry(w, netuid, targetHotkey, attemptNum + 1, actualMaxRetries, actualTimeoutMs, actualAmount, actualSlippageLimit, actualTip, label).then(resolve);
          } else {
            resolve(res);
          }
        }
      });
    });

    if (attemptNum === 1) {
      p.finally(() => {
        activeTimeoutRetryNumByWallet.delete(key);
      });
    }

    return p;
  } catch (e) {
    log('ERROR', `[${label}] 超时重试 #${attemptNum} - 钱包【${w.name}】发起异常: ${e.message}`);
    if (attemptNum === 1) {
      activeTimeoutRetryNumByWallet.delete(key);
    }
    return { success: false, error: e.message };
  }
}

// Staking Sniping execution (pure staking buy-in on newly registered subnet)
async function executeStakingSniping(netuid, hotkey, triggerSource = 'Unknown') {
  const settings = database.getSettings();
  const activeWallets = wallets.filter(w => w.enabled !== false);
  if (activeWallets.length === 0) {
    log('WARN', `[新子网打新] [触发源: ${triggerSource}] 触发打新抢购，但没有加载启用任何小号钱包！`);
    return;
  }

  // 1. 防重复运行锁（基于 netuid）：如果当前子网的打新循环已在运行，直接拦截
  if (activeSnipesByNetuid.has(netuid)) {
    log('INFO', `[新子网打新] [触发源: ${triggerSource}] 子网 #${netuid} 打新抢购循环已经在运行中，跳过重复触发。`);
    return;
  }

  // 优先用传入的 hotkey（从 pending registerNetwork 中提取的），否则回退使用 resolveHotkey
  let targetHotkey = hotkey;
  if (!targetHotkey) {
    log('INFO', `[新子网打新] [触发源: ${triggerSource}] 未提取到 mempool hotkey，启动链上检索...`);
    targetHotkey = await resolveHotkey(netuid);
  }

  if (!targetHotkey) {
    log('WARN', `[新子网打新] [触发源: ${triggerSource}] 无法为子网 #${netuid} 解析到有效 hotkey，打新抢购取消。`);
    return;
  }

  const successKey = `新子网打新:${netuid}:${targetHotkey}`;
  
  // 2. 24小时冷却时间校验（持久化，不受重启影响，不管成功与否）
  const cooldownKey = `new-subnet:${netuid}`;
  const cooldown = database.getCooldown(cooldownKey);
  let shouldWriteCooldown = false;

  if (cooldown) {
    const elapsed = Date.now() - cooldown.firstTriggeredAt;
    if (elapsed < 24 * 60 * 60 * 1000) {
      if (Math.abs(currentBlockHeight - cooldown.block) <= 10) {
        shouldWriteCooldown = false; // 同一次事件兜底，不刷新冷却
      } else {
        log('INFO', `[新子网打新] [触发源: ${triggerSource}] 检测到子网 #${netuid} 在 24 小时冷却时间内 (上次打新区块: #${cooldown.block}, 当前区块: #${currentBlockHeight})，且已超过防抖窗口，跳过重复触发。`);
        return;
      }
    } else {
      // 冷却已过，清理可能残留的内存成功状态
      dashingSuccessByNetuid.delete(successKey);
      shouldWriteCooldown = true;
    }
  } else {
    dashingSuccessByNetuid.delete(successKey);
    shouldWriteCooldown = true;
  }

  // 3. 内存成功状态校验：若之前已有该子网的成功购买记录，直接拦截退出，防止重复买入
  if (dashingSuccessByNetuid.get(successKey) === true) {
    log('INFO', `[新子网打新] 检测到子网 #${netuid} (Hotkey: ${targetHotkey}) 已经打新成功，跳过执行。`);
    return;
  }

  // 写入冷却时间（不论成功与否均冷却 24 小时，同一次防抖窗口内不重复刷新）
  if (shouldWriteCooldown) {
    const ok = database.setCooldown(cooldownKey, {
      strategy: 'new-subnet',
      netuid: netuid,
      block: currentBlockHeight,
      hotkey: targetHotkey
    });
    if (!ok) {
      log('WARN', `[新子网打新] 冷却状态写入失败: key = ${cooldownKey}`);
    }
  }

  // 加锁，并初始化/确保成功标记
  activeSnipesByNetuid.add(netuid);
  if (dashingSuccessByNetuid.get(successKey) === undefined) {
    dashingSuccessByNetuid.set(successKey, false);
  }

  // 收集所有异步发出的交易 Promise，以实现动态解锁
  const txPromises = [];

  const targetTip = calculateDynamicTip(netuid, settings.dashingTip);
  const burstCount = Math.max(1, settings.dashingBurstCount || 1);
  const amountBigInt = BigInt(Math.floor(settings.dashingAmount * 1e9));
  const retries = Math.max(1, settings.dashingRetries || 10);
  const interval = Math.max(50, settings.dashingIntervalMs || 1000);

  log('INFO', `[新子网打新] [触发源: ${triggerSource}] 启动极速打新抢购机制 -> 目标子网 #${netuid}, 目标 Hotkey: ${targetHotkey}, 最大扫射轮数: ${retries}, 扫射间隔: ${interval}ms`);
  sendTelegramAlert(`🚀 [新子网打新 极速启动]\n触发源: ${triggerSource}\n子网: #${netuid}\n目标 Hotkey: ${targetHotkey}\n单轮并发数: ${burstCount}\n最大扫射轮数: ${retries}轮\n扫射间隔: ${interval}ms`);

  try {
    for (let attempt = 0; attempt < retries; attempt++) {
      // 只有在尚未有任何一笔成功购买交易时，才进行新一轮的买入尝试
      if (attempt > 0 && dashingSuccessByNetuid.get(successKey)) {
        log('INFO', `[新子网打新] 检测到已有并发购买交易成功上链，自动终止后续的第 ${attempt + 1}/${retries} 轮扫射。`);
        break;
      }

      log('INFO', `[新子网打新] 开始执行第 ${attempt + 1}/${retries} 轮扫射尝试...`);
      
      for (const w of activeWallets) {
        for (let i = 0; i < burstCount; i++) {
          try {
            const tx = await buildStakeTx(targetHotkey, netuid, amountBigInt, settings.dashingSlippageLimit);
            log('INFO', `[新子网打新] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1}/${burstCount} 笔购买交易发起...`);
            
            // 并发或重试时，每次都会调用 reserveNonce(address) 分配递增的新 nonce 供节点队列式打包
            const p = sendTx(tx, w.pair, settings.dashingTimeoutMs, targetTip, {
              netuid,
              hotkey: targetHotkey,
              amountBigInt,
              slippageLimit: settings.dashingSlippageLimit,
              label: `新子网打新-轮次${attempt + 1}-并发#${i + 1}`
            }).then(res => {
              if (res.success) {
                log('SUCCESS', `[新子网打新] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔购买成功！Hash: ${res.hash}`);
                // 标记成功，用于终止其它重试轮次以及区块头触发的兜底机制
                dashingSuccessByNetuid.set(successKey, true);
                pendingNewSubnet = null; // 清除内存兜底，防重复触发
                sendTelegramAlert(`✅ [新子网打新 成功]\n钱包: ${w.name}\n子网: #${netuid}\n轮次: ${attempt + 1}\n并发索引: ${i + 1}\n交易哈希: ${res.hash}`);
                return res;
              } else {
                log('ERROR', `[新子网打新] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔交易失败: ${res.error}`);
                if (res.error && (res.error.includes('timeout') || res.error.includes('Timeout')) && settings.dashingTimeoutRetries > 0) {
                  return executeTimeoutRetry(w, netuid, targetHotkey, 1, settings.dashingTimeoutRetries, settings.dashingTimeoutMs, settings.dashingAmount, settings.dashingSlippageLimit, settings.dashingTip, '新子网打新');
                }
                return res;
              }
            });
            txPromises.push(p);
          } catch (e) {
            log('ERROR', `[新子网打新] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔交易抛出异常: ${e.message}`);
          }
        }
      }

      if (attempt < retries - 1) {
        await new Promise(resolve => setTimeout(resolve, interval));
      }
    }
  } finally {
    const unlock = () => {
      activeSnipesByNetuid.delete(netuid);
    };

    if (txPromises.length > 0) {
      // 1. 正常通过 Promise.allSettled 释放锁
      Promise.allSettled(txPromises).finally(unlock);
      
      // 2. 超时兜底释放（例如 3分钟）：如果因未知原因 Promise 挂起，强制解锁防止永久死锁
      setTimeout(unlock, 180000); 
    } else {
      // 若一笔交易都未发出（例如 buildStakeTx 抛错），延迟短窗口解锁，防止下个区块到达时瞬间再次重复触发
      setTimeout(unlock, Math.max(3000, interval));
    }
  }
}

// 100% 独立链上自愈检测：检测是否有新注册/被接管回收的子网，并执行兜底抢单
async function detectNewSubnetOnChain(blockHeight) {
  if (!api || !api.isConnected) return;
  try {
    // 1. 从缓存中获取已知的子网列表
    const cachedNetuids = Array.from(subnetRegisteredAtCache.keys());
    
    // 如果缓存为空（例如启动时加载失败），构建默认的查询列表 (0 到 32)
    const netuidsToQuery = cachedNetuids.length > 0 
      ? [...cachedNetuids] 
      : Array.from({ length: 33 }, (_, i) => i);
      
    // 2. 预测并包含下一个可能新增的 netuid (maxCached + 1)
    const maxCached = cachedNetuids.length > 0 ? Math.max(...cachedNetuids) : -1;
    if (maxCached >= 0 && maxCached < 256) {
      netuidsToQuery.push(maxCached + 1);
    }
    
    // 3. 仅用一个批处理 RPC 查询所有这些子网的最新注册区块
    const registeredBlocks = await api.query.subtensorModule.networkRegisteredAt.multi(netuidsToQuery);
    
    for (let i = 0; i < netuidsToQuery.length; i++) {
      const netuid = netuidsToQuery[i];
      const regBlockVal = registeredBlocks[i];
      if (!regBlockVal || regBlockVal.isEmpty) continue;
      
      const regBlock = Number(regBlockVal.toString());
      if (regBlock === 0) continue;
      
      // 获取该子网已缓存的注册区块号
      const cachedRegBlock = subnetRegisteredAtCache.get(netuid);
      
      // 如果注册区块与当前区块相同，且（缓存中不存在该子网，或缓存的注册区块小于最新注册区块）
      // 这说明：要么是一个全新的 netuid，要么是一个被接管回收(recycled)的 netuid！
      if (regBlock === blockHeight && (!cachedRegBlock || cachedRegBlock < regBlock)) {
        // 且该子网打新当前未在运行，立即进行链上兜底抢单！
        if (!activeSnipesByNetuid.has(netuid)) {
          log('SUCCESS', `[新子网打新] 链上自愈检测到子网 #${netuid} 在区块 #${blockHeight} 刚刚被注册/接管！(缓存区块: ${cachedRegBlock || '无'} -> 当前区块: ${regBlock})。触发 100% 独立自愈兜底抢单！`);
          
          // 强行从链上获取最新 hotkey（绕过并强制刷新缓存，因为 recycled 子网的 hotkey 必定变了）
          const targetHotkey = await resolveHotkey(netuid, true);
          if (targetHotkey) {
            // 立即手动更新本地缓存以防同一区块内可能的其他异步检测造成竞态触发
            subnetRegisteredAtCache.set(netuid, regBlock);
            subnetHotkeysCache.set(netuid, targetHotkey);
            
            executeStakingSniping(netuid, targetHotkey, 'Block-Fallback').catch(e => {
              log('ERROR', `[新子网打新] 触发链上兜底抢跑失败: ${e.message}`);
            });
          } else {
            log('WARN', `[新子网打新] 无法为子网 #${netuid} 检索到有效 Hotkey，兜底取消。`);
          }
        }
      }
    }
  } catch (e) {
    log('ERROR', `[新子网打新] 链上自愈检测发生异常: ${e.message}`);
  }
}

// Mempool poll logic
async function poll() {
  if (isPolling || !api || !api.isConnected) return;
  isPolling = true;
  try {
    const pendingHexs = await api.rpc.author.pendingExtrinsics();
    if (!pendingHexs || pendingHexs.length === 0) {
      maxTipBySubnet.clear();
      maxRegisterNetworkTip = 0;
      return;
    }
    
    const now = Date.now();
    
    maxTipBySubnet.clear();
    let currentMaxRegisterTip = 0;
    
    // Seen hashes TTL cleanup (5 minutes window)
    for (const [hash, entry] of seenHashes.entries()) {
      const timestamp = (entry && typeof entry === 'object') ? entry.timestamp : entry;
      if (now - timestamp > 5 * 60 * 1000) seenHashes.delete(hash);
    }
    
    // Seen actions TTL cleanup (10 minutes window)
    for (const [action, timestamp] of seenActions.entries()) {
      if (now - timestamp > 10 * 60 * 1000) seenActions.delete(action);
    }

    for (const hex of pendingHexs) {
      try {
        let ext;
        if (hex && typeof hex.toHex === 'function') {
          ext = hex;
        } else {
          ext = api.createType('Extrinsic', hex.toString());
        }
        
        const parsed = parseExtrinsic(ext);
        if (!parsed) continue;

        let netuid = null;
        if (parsed.args.netuid !== undefined) {
          netuid = Number(parsed.args.netuid.toString());
        } else if (parsed.args.destination_netuid !== undefined) {
          netuid = Number(parsed.args.destination_netuid.toString());
        } else if (parsed.args[1] !== undefined && typeof parsed.args[1].toNumber === 'function') {
          netuid = parsed.args[1].toNumber();
        }

        if (netuid !== null && parsed.tipTao > 0) {
          const currentMax = maxTipBySubnet.get(netuid) || 0;
          if (parsed.tipTao > currentMax) {
            maxTipBySubnet.set(netuid, parsed.tipTao);
          }
        }

        const isReg = /^register(_)?network$/i.test(parsed.callName);
        if (isReg && parsed.tipTao > 0) {
          if (parsed.tipTao > currentMaxRegisterTip) {
            currentMaxRegisterTip = parsed.tipTao;
          }
        }

        const hashEntry = seenHashes.get(parsed.txHash);
        if (hashEntry) {
          if (hashEntry.handled) continue;
          if (now - hashEntry.timestamp < 3000) continue; // 限制失败交易重试频率为最快每 3 秒一次，防止高频砸 RPC 节点和刷警告日志
        }

        if (/^subtensor(Module)?$/i.test(parsed.section) && 
            /^(registerNetwork|register_network|setSubnetIdentity|set_subnet_identity|announceColdkeySwap|announce_coldkey_swap|addStake|addStakeLimit|add_stake|add_stake_limit)$/i.test(parsed.callName)) {
          const handled = await handlePendingExtrinsic(parsed);
          seenHashes.set(parsed.txHash, {
            timestamp: now,
            netuid,
            tipTao: parsed.tipTao,
            isRegisterNetwork: isReg,
            handled: !!handled
          });
        } else {
          seenHashes.set(parsed.txHash, {
            timestamp: now,
            netuid,
            tipTao: parsed.tipTao,
            isRegisterNetwork: isReg,
            handled: true
          });
        }
      } catch (err) {
        // Silent error
      }
    }
    
    // RBF Speed-Up Check
    const settings = database.getSettings();
    if (settings.replaceByFeeEnabled && activePendingTxs.size > 0) {
      for (const [nonceKey, pending] of activePendingTxs.entries()) {
        if (now - pending.sentAt > 60 * 1000) {
          activePendingTxs.delete(nonceKey);
          continue;
        }

        const maxMempoolTip = maxTipBySubnet.get(pending.netuid) || 0;
        if (maxMempoolTip > pending.tip) {
          const targetTip = maxMempoolTip + (settings.replaceByFeeMinDelta || 0.1);
          
          activePendingTxs.delete(nonceKey);
          log('INFO', `[RBF交易加速] 监测到子网 #${pending.netuid} 竞争对手小费 (${maxMempoolTip.toFixed(2)} TAO) 超过我们 (${pending.tip.toFixed(2)} TAO)。正在使用相同 Nonce (${pending.nonce}) 以更高小费 (${targetTip.toFixed(2)} TAO) 加速重发！`);
          
          buildStakeTx(pending.hotkey, pending.netuid, pending.amountBigInt, pending.slippageLimit).then(newTx => {
            sendTx(newTx, pending.pair, 15000, targetTip, {
              nonce: pending.nonce,
              netuid: pending.netuid,
              hotkey: pending.hotkey,
              amountBigInt: pending.amountBigInt,
              slippageLimit: pending.slippageLimit,
              label: `${pending.label}(RBF加速)`
            });
          }).catch(e => {
            log('ERROR', `[RBF交易加速] 重建加速交易失败: ${e.message}`);
          });
        }
      }
    }

    maxRegisterNetworkTip = currentMaxRegisterTip;
  } catch (e) {
    const now = Date.now();
    if (now - lastMempoolErrorTime > 60000) { // Log once per minute to prevent flooding
      lastMempoolErrorTime = now;
      const errMsg = e.message || String(e);
      if (errMsg.includes('unsafe') || errMsg.includes('Method not found') || errMsg.includes('reject') || errMsg.includes('forbidden') || errMsg.includes('unauthorized')) {
        log('ERROR', `[交易池监听失败] 节点拒绝了 pendingExtrinsics 请求！原因: "${errMsg}"。极大概率是因为本地节点未启用 Unsafe RPC 方法。请确保 Subtensor 节点配置了 --rpc-methods=Unsafe !`);
      } else {
        log('WARN', `获取交易池 Pending 交易失败: ${errMsg}`);
      }
    }
  } finally {
    isPolling = false;
  }
}

// Uptime helper
function getUptimeSeconds() {
  if (!systemUptimeStart) return 0;
  return Math.floor((Date.now() - systemUptimeStart) / 1000);
}

// Schedule automatic reconnection
function scheduleReconnect() {
  if (botStatus === 'Stopped') return;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  log('INFO', '将在 5 秒后尝试重新连接 API 节点...');
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    if (botStatus !== 'Stopped') {
      connectWs('Auto Reconnect').catch(e => {
        log('ERROR', `自动重连异常: ${e.message}`);
      });
    }
  }, 5000);
}

// Disconnect helper specifically for triggering reconnect flow
function disconnectForReconnect(reason) {
  log('WARN', `因 ${reason} 断开连接，准备自动重连...`);
  
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
  if (latencyTimer) {
    clearInterval(latencyTimer);
    latencyTimer = null;
  }
  if (broadcastLatencyTimer) {
    clearInterval(broadcastLatencyTimer);
    broadcastLatencyTimer = null;
  }
  for (const provider of broadcastProviders.values()) {
    try { provider.disconnect(); } catch (e) {}
  }
  broadcastProviders.clear();
  broadcastStatuses.clear();
  
  if (api) {
    try { api.disconnect(); } catch (e) {}
    api = null;
  }
  provider = null;
  currentActiveNode = 'Disconnected';
  currentLatency = -1;
  systemUptimeStart = null;
  activeTimeoutRetryNumByWallet.clear();
  
  connectGeneration++;
  isConnecting = false;
  
  botStatus = 'Error';
  scheduleReconnect();
}

// Main Connection Routine
async function connectWs(reason = 'Normal Boot') {
  if (isConnecting) {
    log('INFO', `已经有一个连接流程在运行中，跳过本次连接请求 [原因: ${reason}]`);
    return;
  }
  isConnecting = true;
  const generation = ++connectGeneration;

  botStatus = 'Starting';
  log('INFO', `正在建立连接 [触发原因: ${reason}]...`);
  
  const settings = database.getSettings();
  const targets = [settings.primaryNode, settings.backupNode].filter(Boolean);
  
  if (targets.length === 0) {
    botStatus = 'Error';
    log('ERROR', '未配置任何 API 节点，请检查系统设置！');
    isConnecting = false;
    return;
  }

  let connected = false;
  for (const url of targets) {
    if (generation !== connectGeneration || botStatus === 'Stopped') {
      isConnecting = false;
      return;
    }
    try {
      log('INFO', `尝试连接节点: ${url}...`);
      currentActiveNode = url;
      
      provider = new WsProvider(url, false);
      
      const connPromise = new Promise((resolve, reject) => {
        provider.on('connected', () => resolve(true));
        provider.on('error', (err) => reject(err));
        provider.connect();
      });
      
      const timeoutPromise = new Promise((_, reject) => setTimeout(() => reject(new Error('Connection Timeout')), 6000));
      
      await Promise.race([connPromise, timeoutPromise]);
      if (generation !== connectGeneration || botStatus === 'Stopped') {
        try { provider.disconnect(); } catch (err) {}
        provider = null;
        isConnecting = false;
        return;
      }
      
      api = await ApiPromise.create({ provider });
      if (generation !== connectGeneration || botStatus === 'Stopped') {
        if (api) {
          try { await api.disconnect(); } catch (err) {}
          api = null;
        }
        provider = null;
        isConnecting = false;
        return;
      }
      
      log('SUCCESS', `成功连接至节点: ${url} (链名称: ${api.runtimeChain || 'Subtensor'}, Spec版本: ${api.runtimeVersion?.specVersion || 'unknown'}, 创世哈希: ${api.genesisHash?.toHex().slice(0, 10)}...)`);
      connected = true;
      break;
    } catch (e) {
      log('WARN', `连接节点 ${url} 失败: ${e.message}`);
      if (api) {
        try { await api.disconnect(); } catch (err) {}
        api = null;
      }
      provider = null;
    }
  }

  if (!connected) {
    botStatus = 'Error';
    log('ERROR', '所有配置的 API 节点均连接失败！');
    currentActiveNode = 'Disconnected';
    currentLatency = -1;
    systemUptimeStart = null;
    isConnecting = false;
    scheduleReconnect();
    return;
  }

  try {
    botStatus = 'Running';
    if (!systemUptimeStart) systemUptimeStart = Date.now();

    if (generation !== connectGeneration || botStatus === 'Stopped') return;
    try {
      log('INFO', '[API初始化] 正在测试本地节点是否开启 Unsafe RPC 接口 (用于交易池 Pending 交易扫描)...');
      await api.rpc.author.pendingExtrinsics();
      if (generation !== connectGeneration || botStatus === 'Stopped') return;
      log('SUCCESS', '本地交易池 (Mempool) 监听接口测试成功：Unsafe RPC 已启用！');
    } catch (err) {
      if (generation !== connectGeneration || botStatus === 'Stopped') return;
      const errMsg = err.message || String(err);
      log('WARN', `[注意] 本地交易池监听接口测试失败: "${errMsg}"。如果该节点是你的交易网关，请确保节点启动命令配置了 --rpc-methods=Unsafe，否则机器人将无法监听 Pending 交易！`);
    }

    log('INFO', '[API初始化] 正在加载本地小号钱包并同步链上 Nonce 与余额...');
    await reloadWallets();
    if (generation !== connectGeneration || botStatus === 'Stopped') return;
    
    log('INFO', '[API初始化] 正在批量拉取链上所有活跃子网的注册区块、Owner 和 Hotkey 信息...');
    await refreshSubnetOwnersCache();
    if (generation !== connectGeneration || botStatus === 'Stopped') return;

    log('INFO', '[API初始化] 正在配置多节点广播组件并启动节点延迟测试...');
    initBroadcastNodes();
    if (generation !== connectGeneration || botStatus === 'Stopped') return;
    if (broadcastLatencyTimer) clearInterval(broadcastLatencyTimer);
    broadcastLatencyTimer = setInterval(testBroadcastNodes, 10000);
    setTimeout(testBroadcastNodes, 2000);

    log('SUCCESS', '[API初始化] 节点连接与全部初始化请求执行完毕！机器人正式进入 RUNNING 状态，已启动区块头 (subscribeNewHeads) 监听！');

    if (generation !== connectGeneration || botStatus === 'Stopped') return;
    api.rpc.chain.subscribeNewHeads(async (header) => {
      if (generation !== connectGeneration || botStatus === 'Stopped') return;
      const blockNumber = header.number.toNumber();
      currentBlockHeight = blockNumber;
      if (global.blockCallback) {
        global.blockCallback(blockNumber);
      }

      // 执行冷键与改名交易的区块自愈检测，传入高度常量避免日志错位
      detectEventsInBlock(header.hash, blockNumber).catch(e => {
        log('WARN', `[区块兜底] 解析新区块 #${blockNumber} 失败: ${e.message}`);
      });
      
      const settings = database.getSettings();
      if (settings.dashingEnabled) {
        const runDashingFlow = async () => {
          if (pendingNewSubnet) {
            const snipe = pendingNewSubnet;
            pendingNewSubnet = null;
            if (activeSnipesByNetuid.has(snipe.netuid)) {
              log('INFO', `[新子网打新] 区块头 #${currentBlockHeight} 到达。内存子网 #${snipe.netuid} 已有抢购循环在运行中，跳过内存兜底触发。`);
            } else {
              log('SUCCESS', `[新子网打新] 检测到新区块头 #${currentBlockHeight} 到达，立刻对内存中的新子网 #${snipe.netuid} (Hotkey: ${snipe.hotkey}) 执行 Staking 抢购！`);
              executeStakingSniping(snipe.netuid, snipe.hotkey, 'Memory-Fallback').catch(e => {
                log('ERROR', `[新子网打新] 触发内存兜底打新抢购失败: ${e.message}`);
              });
            }
            await refreshSubnetOwnersCache();
          } else {
            const hasChange = await detectNewSubnetOnChain(currentBlockHeight);
            if (hasChange || currentBlockHeight % 100 === 0) {
              await refreshSubnetOwnersCache();
            }
          }
        };
        
        runDashingFlow().catch(e => {
          log('WARN', `[新子网打新] 链上自愈检测/缓存同步失败: ${e.message}`);
        });
      } else {
        if (currentBlockHeight % 100 === 0) {
          refreshSubnetOwnersCache().catch(() => {});
        }
      }

      if (pendingSandwichSell) {
        const sell = pendingSandwichSell;
        pendingSandwichSell = null;
        log('INFO', `[三明治套利] 检测到新区块 #${currentBlockHeight}，自动执行后置售出！`);
        await executeSandwichSell(sell.netuid, sell.hotkey, sell.amount, sell.tip);
      }
    });

    const pollInterval = Math.max(50, settings.dashingIntervalMs || 100);
    
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(poll, pollInterval);

    if (latencyTimer) clearInterval(latencyTimer);
    latencyTimer = setInterval(async () => {
      if (!api || !api.isConnected) {
        disconnectForReconnect('Connection Dropped');
        return;
      }
      const start = Date.now();
      try {
        await api.rpc.system.health();
        currentLatency = Date.now() - start;
      } catch (e) {
        currentLatency = -1;
        disconnectForReconnect(`Heartbeat Timeout (${e.message})`);
      }
    }, 10000);

  } catch (e) {
    if (generation === connectGeneration && botStatus !== 'Stopped') {
      botStatus = 'Error';
      log('ERROR', `连接初始化失败: ${e.message}`);
      if (api) {
        try { await api.disconnect(); } catch (err) {}
        api = null;
      }
      provider = null;
      scheduleReconnect();
    }
  } finally {
    if (generation === connectGeneration) {
      isConnecting = false;
    }
  }
}

// Start and Stop control
function startBot() {
  if (botStatus === 'Running' || botStatus === 'Starting') return;
  connectWs('User triggered start').catch(e => {
    botStatus = 'Error';
    log('ERROR', `启动机器人异常: ${e.message}`);
  });
}

// Stop bot control
function stopBot() {
  botStatus = 'Stopped';
  log('INFO', '套利机器人正在关闭...');
  
  connectGeneration++;
  isConnecting = false;
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
  if (latencyTimer) {
    clearInterval(latencyTimer);
    latencyTimer = null;
  }
  if (broadcastLatencyTimer) {
    clearInterval(broadcastLatencyTimer);
    broadcastLatencyTimer = null;
  }
  
  for (const provider of broadcastProviders.values()) {
    try { provider.disconnect(); } catch (e) {}
  }
  broadcastProviders.clear();
  broadcastStatuses.clear();
  
  if (api) {
    try {
      api.disconnect();
    } catch (e) {}
    api = null;
  }
  provider = null;
  currentActiveNode = 'Disconnected';
  currentLatency = -1;
  systemUptimeStart = null;
  activeTimeoutRetryNumByWallet.clear();
  log('INFO', '套利机器人已安全关闭。');
}

// Export API
module.exports = {
  startBot,
  stopBot,
  testTelegram,
  testApiUrl,
  refreshAllWallets,
  reloadWallets,
  getWalletsStatus,
  getUptimeSeconds,
  getLogs: () => logs,
  getStatus: () => ({
    status: botStatus,
    activeNode: currentActiveNode,
    latency: currentLatency,
    blockHeight: currentBlockHeight,
    uptime: getUptimeSeconds(),
    broadcastNodes: getBroadcastNodesStatus(),
    serverTime: Date.now()
  }),
  setLogCallback: (cb) => { global.logCallback = cb; },
  setBlockCallback: (cb) => { global.blockCallback = cb; }
};
