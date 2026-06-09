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
async function reloadWallets() {
  await cryptoWaitReady();
  keyring = new Keyring({ type: 'sr25519' });
  
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
      tipTao
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
let pendingSandwichSell = null;

// Core extrinsic execution triggers
async function handlePendingExtrinsic(parsed) {
  const { callName, args, txHash, signer } = parsed;
  const settings = database.getSettings();
  const now = Date.now();

  const normalizedCall = callName.toLowerCase();

  // 1. registerNetwork / register_network -> 新子网 Staking 抢购
  if (/^register(_)?network$/i.test(normalizedCall)) {
    if (!settings.dashingEnabled) return;
    
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
        const actionKey = `dashing:${targetNetuid}`;
        if (seenActions.has(actionKey)) return;
        seenActions.set(actionKey, now);

        log('INFO', `[新子网打新] 扫到他人提交注册交易。预测 netuid #${targetNetuid}，提取到新子网目标 hotkey: ${targetHotkey}。立即启动极速 Staking 抢单！`);
        
        // 立即执行极速抢单，并在后台进行重试与并发
        executeStakingSniping(targetNetuid, targetHotkey).catch(e => {
          log('ERROR', `[新子网打新] 触发极速抢购失败: ${e.message}`);
        });

        // 链上兜底机制将通过新区块到达时的 detectNewSubnetOnChain 自愈检测自动执行
      } else {
        log('WARN', `[新子网打新] 扫到注册网络交易，但无法解析目标 netuid (${targetNetuid}) 或 hotkey (${targetHotkey})`);
      }
    } catch (e) {
      log('ERROR', `处理新子网打新判断错误: ${e.message}`);
    }
  }

  // 2. setSubnetIdentity / set_subnet_identity -> 子网改名抢跑
  if (/^set(_)?subnet(_)?identity$/i.test(normalizedCall)) {
    if (!settings.renameEnabled) return;
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
          return;
        }

        // 🔒 安全保护 2：如果是全新创建的子网首次起名，跳过改名抢跑以避免与策略 1 重复买入。
        // 我们通过查询内存缓存获取其注册区块年龄（0ms 延迟，不拖慢抢跑速度）
        const registeredAt = subnetRegisteredAtCache.get(netuid) || 0;
        if (registeredAt > 0 && currentBlockHeight - registeredAt < 100) {
          log('INFO', `[改名抢跑] 子网 #${netuid} 为近 ${currentBlockHeight - registeredAt} 个区块内刚创建的新子网（内存判定）。跳过改名抢跑（由策略 1 负责 Staking 抢购），防止重复买入。`);
          return;
        }

        const actionKey = `rename:${netuid}:${cleanName}`;
        if (seenActions.has(actionKey)) return;
        seenActions.set(actionKey, now);

        log('INFO', `[改名抢跑] 扫到子网 #${netuid} 提交改名交易 -> "${cleanName}" (Hash: ${txHash})`);
        
        const targetHotkey = await resolveHotkey(netuid);
        if (targetHotkey) {
          executeArbitrageStake(netuid, targetHotkey, settings.dashingAmount, settings.renameTip, '改名抢跑', settings.renameSlippageLimit);
        } else {
          log('WARN', `[改名抢跑] 无法为子网 #${netuid} 解析到有效 hotkey，取消抢跑。`);
        }
      }
    } catch (e) {
      log('ERROR', `处理改名抢跑判断错误: ${e.message}`);
    }
  }

  // 3. announceColdkeySwap / swapColdkey -> 冷键交换抢跑
  if (/^(announceColdkeySwap|swapColdkey|announce_coldkey_swap|swap_coldkey|swapColdkeyAnnounced|swap_coldkey_announced)$/i.test(normalizedCall)) {
    if (!settings.swapEnabled) return;
    try {
      let oldColdkey = null;
      if (/^(announceColdkeySwap|announce_coldkey_swap|swapColdkeyAnnounced|swap_coldkey_announced)$/i.test(callName)) {
        oldColdkey = signer;
      } else {
        oldColdkey = args.old_coldkey?.toString() || args[0]?.toString();
      }
      
      if (oldColdkey && oldColdkey !== 'unsigned' && oldColdkey !== 'unknown') {
        log('INFO', `[冷键交换抢跑] 扫到交换冷键声明/执行 -> ${callName} (Old Coldkey: ${oldColdkey})`);

        for (const [netuid, owner] of subnetOwnersCache.entries()) {
          try {
            if (owner === oldColdkey) {
              const actionKey = `swap:${netuid}:${oldColdkey}`;
              if (seenActions.has(actionKey)) continue;
              seenActions.set(actionKey, now);

              log('INFO', `[冷键交换抢跑] 匹配到目标受控子网 #${netuid}，立即执行抢跑！`);
              const targetHotkey = await resolveHotkey(netuid);
              if (targetHotkey) {
                executeArbitrageStake(netuid, targetHotkey, settings.dashingAmount, settings.swapTip, '冷键交换抢跑', settings.swapSlippageLimit);
              } else {
                log('WARN', `[冷键交换抢跑] 无法为子网 #${netuid} 解析到有效 hotkey，取消抢跑。`);
              }
            }
          } catch (e) {}
        }
      }
    } catch (e) {
      log('ERROR', `处理冷键交换抢跑判断错误: ${e.message}`);
    }
  }

  // 4. addStake / addStakeLimit / swapStake / swapStakeLimit -> 大额买入三明治套利
  if (/^(add(_)?stake|add(_)?stake(_)?limit|swap(_)?stake|swap(_)?stake(_)?limit)$/i.test(normalizedCall)) {
    if (!settings.sandwichEnabled) return;
    try {
      let netuid = null;
      let hotkey = null;
      let amountRao = 0n;
      let isBuy = false;

      if (/^add(_)?stake/i.test(normalizedCall)) {
        hotkey = args.hotkey?.toString() || args[0]?.toString();
        netuid = Number(args.netuid?.toString() || args[1]?.toString());
        amountRao = BigInt(args.stake_to_be_added?.toString() || args.amount?.toString() || args.amount_staked?.toString() || args[2]?.toString() || '0');
        isBuy = true;
      } else {
        hotkey = args.hotkey?.toString() || args[0]?.toString();
        const originNetuid = Number(args.origin_netuid?.toString() || args.origin?.toString() || args[1]?.toString());
        const destNetuid = Number(args.destination_netuid?.toString() || args.destination?.toString() || args[2]?.toString());
        amountRao = BigInt(args.alpha_amount?.toString() || args.amount?.toString() || args[3]?.toString() || '0');
        
        if (destNetuid > 0) {
          netuid = destNetuid;
          isBuy = true;
        }
      }

      if (isBuy && netuid !== null && hotkey) {
        const amountTao = Number(amountRao) / 1e9;
        if (amountTao >= settings.sandwichThreshold) {
          const actionKey = `sandwich:${txHash}`;
          if (seenActions.has(actionKey)) return;
          seenActions.set(actionKey, now);

          log('WARN', `[三明治套利] 扫到大额买入交易 (金额: ${amountTao.toFixed(2)} TAO, 子网 #${netuid}, 目标 Hotkey: ${hotkey}) (Hash: ${txHash})！`);
          sendTelegramAlert(`🚨 [三明治套利触发]\n检测到大额买入交易！\n金额: ${amountTao.toFixed(2)} TAO\n子网: #${netuid}\n目标 Hotkey: ${hotkey}\n发送者: ${signer}\n正在执行前置抢跑买入...`);

          // Calculate dynamic slippage
          const calculatedSlippage = await calculateDynamicSlippage(netuid, amountTao);
          
          if (settings.dynamicSlippageEnabled) {
            const safetyFactor = settings.dynamicSlippageSafetyFactor || 0.7;
            const expectedRise = calculatedSlippage / safetyFactor;
            if (expectedRise < 0.015) {
              log('INFO', `[三明治套利] 子网 #${netuid} 预测价格涨幅 ${(expectedRise * 100).toFixed(2)}% 过低，无法覆盖套利小费，放弃本次套利。`);
              return;
            }
          }

          const buySuccess = await executeSandwichBuy(netuid, hotkey, settings.dashingAmount, settings.sandwichTip, calculatedSlippage);
          if (buySuccess && settings.sandwichAutoSell) {
            log('INFO', `[三明治套利] 成功执行前置买入，已登记后置卖出，将在下一个区块头确认时自动发起售出...`);
            pendingSandwichSell = {
              netuid,
              hotkey,
              amount: settings.dashingAmount,
              tip: settings.sandwichSellTip
            };
          }
        }
      }
    } catch (e) {
      log('ERROR', `处理大额买入抢跑判断错误: ${e.message}`);
    }
  }
}

// Resolve a valid hotkey for a given subnet using multi-tiered fallback
async function resolveHotkey(netuid) {
  const cached = subnetHotkeysCache.get(netuid);
  if (cached && cached.length >= 47) return cached;

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
  const activeWallets = wallets.filter(w => w.enabled !== false);
  if (activeWallets.length === 0) {
    log('WARN', `[${label}] 触发抢跑，但没有加载启用任何小号钱包！`);
    return;
  }

  const targetTip = calculateDynamicTip(netuid, tip);
  log('INFO', `[${label}] 启动 ${activeWallets.length} 个钱包执行购买，基准金额: ${amountTao} TAO, 最终小费: ${targetTip} TAO`);
  sendTelegramAlert(`🚀 [${label} 触发]\n子网: #${netuid}\n目标 Hotkey: ${hotkey}\n执行钱包数: ${activeWallets.length}`);

  const amountBigInt = BigInt(Math.floor(amountTao * 1e9));
  
  for (const w of activeWallets) {
    try {
      const tx = await buildStakeTx(hotkey, netuid, amountBigInt, slippageLimit);
      log('INFO', `[${label}] 钱包【${w.name}】发起交易...`);
      sendTx(tx, w.pair, 10000, targetTip, {
        netuid,
        hotkey,
        amountBigInt,
        slippageLimit,
        label
      }).then(res => {
        if (res.success) {
          log('SUCCESS', `[${label} 成功] 钱包【${w.name}】购买成功！Hash: ${res.hash}`);
          sendTelegramAlert(`✅ [${label} 成功]\n钱包: ${w.name}\n子网: #${netuid}\n交易哈希: ${res.hash}`);
        } else {
          log('ERROR', `[${label} 失败] 钱包【${w.name}】交易失败: ${res.error}`);
          sendTelegramAlert(`❌ [${label} 失败]\n钱包: ${w.name}\n子网: #${netuid}\n原因: ${res.error}`);
        }
      });
    } catch (e) {
      log('ERROR', `[${label}] 钱包【${w.name}】交易抛出异常: ${e.message}`);
    }
  }
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
    const res = await sendTx(tx, w.pair, 10000, targetTip, {
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
      const stakeVal = await api.query.subtensorModule.stake(hotkey, w.pair.address);
      stakeRao = BigInt(stakeVal.toString());
      if (stakeRao > 0n) break;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  } catch (err) {
    log('WARN', `[三明治套利] 查询 stake 余额异常: ${err.message}`);
  }

  if (stakeRao === 0n) {
    log('WARN', `[三明治套利] 未查询到实际 stake 余额，回退使用估算金额。`);
    stakeRao = BigInt(Math.floor(amountTao * 1e9));
  } else {
    log('INFO', `[三明治套利] 成功获取到实际 Alpha 质押余额: ${(Number(stakeRao) / 1e9).toFixed(4)} Alpha`);
  }
  
  try {
    const tx = await buildUnstakeTx(hotkey, netuid, stakeRao, settings.sandwichSlippageLimit);
    log('INFO', `[三明治套利] 发起后置卖出 -> 钱包: ${w.name}, 数量: ${(Number(stakeRao) / 1e9).toFixed(4)} Alpha, 小费: ${targetTip} TAO`);
    const res = await sendTx(tx, w.pair, 10000, targetTip);
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
async function executeTimeoutRetry(w, netuid, targetHotkey, attemptNum) {
  const settings = database.getSettings();
  const maxTimeoutRetries = settings.dashingTimeoutRetries || 0;
  if (attemptNum > maxTimeoutRetries) return;
  
  const successKey = `${netuid}:${targetHotkey}`;
  if (dashingSuccessByNetuid.get(successKey)) return;
  
  // Prevent duplicate concurrent timeout retries for the same wallet
  const key = `${netuid}:${w.name}`;
  const currentActive = activeTimeoutRetryNumByWallet.get(key) || 0;
  if (attemptNum <= currentActive) return;
  activeTimeoutRetryNumByWallet.set(key, attemptNum);
  
  log('WARN', `[新子网打新] 钱包【${w.name}】交易超时。触发第 ${attemptNum}/${maxTimeoutRetries} 次超时重试...`);
  
  // Wait 1 second before retrying to ensure the nonce query inside sendTx timeout handler completed
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  if (dashingSuccessByNetuid.get(successKey)) return;
  
  try {
    const targetTip = calculateDynamicTip(netuid, settings.dashingTip);
    const amountBigInt = BigInt(Math.floor(settings.dashingAmount * 1e9));
    const tx = await buildStakeTx(targetHotkey, netuid, amountBigInt, settings.dashingSlippageLimit);
    
    sendTx(tx, w.pair, settings.dashingTimeoutMs, targetTip, {
      netuid,
      hotkey: targetHotkey,
      amountBigInt,
      slippageLimit: settings.dashingSlippageLimit,
      label: `新子网打新-超时重试#${attemptNum}`
    }).then(res => {
      if (res.success) {
        log('SUCCESS', `[新子网打新] 超时重试 #${attemptNum} - 钱包【${w.name}】购买成功！Hash: ${res.hash}`);
        dashingSuccessByNetuid.set(successKey, true);
        sendTelegramAlert(`✅ [新子网打新 超时重试成功]\n钱包: ${w.name}\n子网: #${netuid}\n重试次数: ${attemptNum}\n交易哈希: ${res.hash}`);
      } else {
        log('ERROR', `[新子网打新] 超时重试 #${attemptNum} - 钱包【${w.name}】交易失败: ${res.error}`);
        if (res.error && (res.error.includes('timeout') || res.error.includes('Timeout'))) {
          executeTimeoutRetry(w, netuid, targetHotkey, attemptNum + 1);
        }
      }
    });
  } catch (e) {
    log('ERROR', `[新子网打新] 超时重试 #${attemptNum} - 钱包【${w.name}】发起异常: ${e.message}`);
  }
}

// Staking Sniping execution (pure staking buy-in on newly registered subnet)
async function executeStakingSniping(netuid, hotkey) {
  const settings = database.getSettings();
  const activeWallets = wallets.filter(w => w.enabled !== false);
  if (activeWallets.length === 0) {
    log('WARN', `[新子网打新] 触发打新抢购，但没有加载启用任何小号钱包！`);
    return;
  }

  // 1. 防重复运行锁（基于 netuid）：如果当前子网的打新循环已在运行，直接拦截
  if (activeSnipesByNetuid.has(netuid)) {
    log('INFO', `[新子网打新] 子网 #${netuid} 打新抢购循环已经在运行中，跳过重复触发。`);
    return;
  }

  // 优先用传入的 hotkey（从 pending registerNetwork 中提取的），否则回退使用 resolveHotkey
  let targetHotkey = hotkey;
  if (!targetHotkey) {
    log('INFO', `[新子网打新] 未提取到 mempool hotkey，启动链上检索...`);
    targetHotkey = await resolveHotkey(netuid);
  }

  if (!targetHotkey) {
    log('WARN', `[新子网打新] 无法为子网 #${netuid} 解析到有效 hotkey，打新抢购取消。`);
    return;
  }

  const successKey = `${netuid}:${targetHotkey}`;
  
  // 2. 成功状态校验：若之前已有该子网的成功购买记录，直接拦截退出，防止重复买入
  if (dashingSuccessByNetuid.get(successKey) === true) {
    log('INFO', `[新子网打新] 检测到子网 #${netuid} (Hotkey: ${targetHotkey}) 已经打新成功，跳过执行。`);
    return;
  }

  // 加锁，并初始化/确保成功标记
  activeSnipesByNetuid.add(netuid);
  if (dashingSuccessByNetuid.get(successKey) === undefined) {
    dashingSuccessByNetuid.set(successKey, false);
  }

  const targetTip = calculateDynamicTip(netuid, settings.dashingTip);
  const burstCount = Math.max(1, settings.dashingBurstCount || 1);
  const amountBigInt = BigInt(Math.floor(settings.dashingAmount * 1e9));
  const retries = Math.max(1, settings.dashingRetries || 10);
  const interval = Math.max(50, settings.dashingIntervalMs || 1000);

  log('INFO', `[新子网打新] 启动极速打新抢购机制 -> 目标子网 #${netuid}, 目标 Hotkey: ${targetHotkey}, 重试轮数: ${retries}, 间隔: ${interval}ms`);
  sendTelegramAlert(`🚀 [新子网打新 极速启动]\n子网: #${netuid}\n目标 Hotkey: ${targetHotkey}\n每轮并发数: ${burstCount}\n最大重试: ${retries}轮\n重试间隔: ${interval}ms`);

  try {
    for (let attempt = 0; attempt < retries; attempt++) {
      // 只有在尚未有任何一笔成功购买交易时，才进行新一轮的买入尝试
      if (attempt > 0 && dashingSuccessByNetuid.get(successKey)) {
        log('INFO', `[新子网打新] 检测到已有并发购买交易成功上链，自动终止后续的第 ${attempt + 1}/${retries} 轮重试。`);
        break;
      }

      log('INFO', `[新子网打新] 开始执行第 ${attempt + 1}/${retries} 轮购买尝试...`);
      
      for (const w of activeWallets) {
        for (let i = 0; i < burstCount; i++) {
          try {
            const tx = await buildStakeTx(targetHotkey, netuid, amountBigInt, settings.dashingSlippageLimit);
            log('INFO', `[新子网打新] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1}/${burstCount} 笔购买交易发起...`);
            
            // 并发或重试时，每次都会调用 reserveNonce(address) 分配递增的新 nonce 供节点队列式打包
            sendTx(tx, w.pair, settings.dashingTimeoutMs, targetTip, {
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
                sendTelegramAlert(`✅ [新子网打新 成功]\n钱包: ${w.name}\n子网: #${netuid}\n轮次: ${attempt + 1}\n并发索引: ${i + 1}\n交易哈希: ${res.hash}`);
              } else {
                log('ERROR', `[新子网打新] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔交易失败: ${res.error}`);
                if (res.error && (res.error.includes('timeout') || res.error.includes('Timeout')) && settings.dashingTimeoutRetries > 0) {
                  executeTimeoutRetry(w, netuid, targetHotkey, 1);
                }
              }
            });
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
    // 整个抢购周期退出后（成功、达到重试上限或出现致命错误），解锁以允许后续可能的新子网操作
    activeSnipesByNetuid.delete(netuid);
  }
}

// 100% 独立链上自愈检测：检测是否有新注册/被接管回收的子网，并执行兜底抢单
async function detectNewSubnetOnChain(blockHeight) {
  if (!api || !api.isConnected) return;
  try {
    const netuidKeys = await api.query.subtensorModule.networksAdded.keys();
    const activeNetuids = netuidKeys.map(({ args: [netuid] }) => netuid.toNumber());
    
    // 批量查询所有子网的注册区块号
    const registeredBlocks = await api.query.subtensorModule.networkRegisteredAt.multi(activeNetuids);
    
    for (let i = 0; i < activeNetuids.length; i++) {
      const netuid = activeNetuids[i];
      const regBlockVal = registeredBlocks[i];
      if (!regBlockVal || regBlockVal.isEmpty) continue;
      
      const regBlock = Number(regBlockVal.toString());
      // 如果注册区块就是当前块，说明在这个块上发生了注册或接管回收！
      if (regBlock === blockHeight) {
        // 且该子网打新循环当前未在运行，则说明漏单了，立即进行链上兜底抢单！
        if (!activeSnipesByNetuid.has(netuid)) {
          log('SUCCESS', `[新子网打新] 链上自愈检测到子网 #${netuid} 在区块 #${blockHeight} 刚刚被注册/接管！触发 100% 独立自愈兜底抢单！`);
          
          // 查询 hotkey
          const targetHotkey = await resolveHotkey(netuid);
          if (targetHotkey) {
            executeStakingSniping(netuid, targetHotkey).catch(e => {
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
    if (!pendingHexs || pendingHexs.length === 0) return;
    
    const now = Date.now();
    
    maxTipBySubnet.clear();
    let currentMaxRegisterTip = 0;
    
    // Seen hashes TTL cleanup (5 minutes window)
    for (const [hash, timestamp] of seenHashes.entries()) {
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

        if (/^register(_)?network$/i.test(parsed.callName) && parsed.tipTao > 0) {
          if (parsed.tipTao > currentMaxRegisterTip) {
            currentMaxRegisterTip = parsed.tipTao;
          }
        }

        if (seenHashes.has(parsed.txHash)) continue;
        seenHashes.set(parsed.txHash, now);

        if (/^subtensor(Module)?$/i.test(parsed.section) && 
            /^(registerNetwork|register_network|setSubnetIdentity|set_subnet_identity|announceColdkeySwap|announce_coldkey_swap|swapColdkey|swap_coldkey|swapColdkeyAnnounced|swap_coldkey_announced|addStake|addStakeLimit|add_stake|add_stake_limit|swapStake|swapStakeLimit|swap_stake|swap_stake_limit)$/i.test(parsed.callName)) {
          await handlePendingExtrinsic(parsed);
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

// Main Connection Routine
async function connectWs(reason = 'Normal Boot') {
  botStatus = 'Starting';
  log('INFO', `正在建立连接 [触发原因: ${reason}]...`);
  
  const settings = database.getSettings();
  const targets = [settings.primaryNode, settings.backupNode].filter(Boolean);
  
  if (targets.length === 0) {
    botStatus = 'Error';
    log('ERROR', '未配置任何 API 节点，请检查系统设置！');
    return;
  }

  let connected = false;
  for (const url of targets) {
    try {
      log('INFO', `尝试连接节点: ${url}...`);
      currentActiveNode = url;
      
      provider = new WsProvider(url, false); // Do not auto-reconnect inside provider to manage fallback manually
      
      // Setup connection timeout
      const connPromise = new Promise((resolve, reject) => {
        provider.on('connected', () => resolve(true));
        provider.on('error', (err) => reject(err));
        provider.connect();
      });
      
      const timeoutPromise = new Promise((_, reject) => setTimeout(() => reject(new Error('Connection Timeout')), 6000));
      
      await Promise.race([connPromise, timeoutPromise]);
      
      api = await ApiPromise.create({ provider });
      
      log('SUCCESS', `成功连接至节点: ${url}`);
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
    log('ERROR', '所有配置的 API 节点均连接失败！进入关断模式。');
    currentActiveNode = 'Disconnected';
    currentLatency = -1;
    systemUptimeStart = null; // Reset uptime to 0
    return;
  }

  botStatus = 'Running';
  if (!systemUptimeStart) systemUptimeStart = Date.now();

  // Test if unsafe RPC is enabled (required for mempool scanning)
  try {
    await api.rpc.author.pendingExtrinsics();
    log('SUCCESS', '本地交易池 (Mempool) 监听接口测试成功：Unsafe RPC 已启用！');
  } catch (err) {
    const errMsg = err.message || String(err);
    log('WARN', `[注意] 本地交易池监听接口测试失败: "${errMsg}"。如果该节点是你的交易网关，请确保节点启动命令配置了 --rpc-methods=Unsafe，否则机器人将无法监听 Pending 交易！`);
  }

  // Load wallets keypairs dynamically
  await reloadWallets();
  // Initialize subnet owners cache
  await refreshSubnetOwnersCache();

  // Initialize broadcast nodes and start latency tester
  initBroadcastNodes();
  if (broadcastLatencyTimer) clearInterval(broadcastLatencyTimer);
  broadcastLatencyTimer = setInterval(testBroadcastNodes, 10000);
  setTimeout(testBroadcastNodes, 2000);

  // Subscribe to block headers
  api.rpc.chain.subscribeNewHeads(async (header) => {
    currentBlockHeight = header.number.toNumber();
    if (global.blockCallback) {
      global.blockCallback(currentBlockHeight);
    }
    
    // 100% 独立链上自愈检测：检测是否有新注册/回收的子网，并执行兜底抢跑
    const settings = database.getSettings();
    if (settings.dashingEnabled) {
      detectNewSubnetOnChain(currentBlockHeight).catch(e => {
        log('WARN', `[新子网打新] 链上自愈检测失败: ${e.message}`);
      });
    }

    // Check pending Sandwich Sell (Backrun)
    if (pendingSandwichSell) {
      const sell = pendingSandwichSell;
      pendingSandwichSell = null;
      log('INFO', `[三明治套利] 检测到新区块 #${currentBlockHeight}，自动执行后置售出！`);
      await executeSandwichSell(sell.netuid, sell.hotkey, sell.amount, sell.tip);
    }

    // Refresh subnet owners cache asynchronously in the background for the next block
    refreshSubnetOwnersCache().catch(() => {});
  });

  // Start Mempool Polling
  const pollInterval = Math.max(50, settings.dashingIntervalMs || 100);
  
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(poll, pollInterval);

  // Start Latency loop
  if (latencyTimer) clearInterval(latencyTimer);
  latencyTimer = setInterval(async () => {
    if (!api || !api.isConnected) {
      log('WARN', '检测到 API 节点连接断开，正在尝试自动重连...');
      stopBot();
      connectWs('Connection Dropped');
      return;
    }
    const start = Date.now();
    try {
      await api.rpc.system.health();
      currentLatency = Date.now() - start;
    } catch (e) {
      currentLatency = -1;
      log('WARN', `API 节点通信无响应 (心跳超时): ${e.message}，尝试重连...`);
      stopBot();
      connectWs('Heartbeat Timeout');
    }
  }, 10000);
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
  
  // Clean up all broadcast connections
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
  systemUptimeStart = null; // Reset uptime to 0
  activeTimeoutRetryNumByWallet.clear(); // Clear timeout retry tracking map on shutdown
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
