const { ApiPromise, WsProvider, Keyring } = require('@polkadot/api');
const { cryptoWaitReady } = require('@polkadot/util-crypto');
const axios = require('axios');
const WebSocket = require('ws');
const database = require('./database');
const preheater = require('./preheater');
const flashduty = require('./flashduty');
const nonceSync = require('./nonceSync');
const fs = require('fs');
const path = require('path');
const privateWallet = require('./privateWallet');


// Logs buffer and state
const logs = [];
const maxLogs = 2000;
let api = null;
let provider = null;
let botStatus = 'Stopped'; // 'Stopped', 'Starting', 'Running', 'Error'
let currentActiveNode = '';
let currentLatency = -1;
let currentBlockHeight = 0;
let cachedBlockHash = null; // 缓存当前区块 hash,供 mortal era 签名用
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
let lastTtlCleanupTime = 0; // 新增：用于限制 TTL 内存清理频率

// Mempool maps for deduplication and nonce tracking
const seenHashes = new Map();
const seenActions = new Map();
const nextNonceByAddress = new Map();
const inFlightNonces = new Map();
const balanceByAddress = new Map();
const processedNetuids = new Map();
const dashingSuccessByNetuid = new Map(); // added for tracking successful snipes
const activeSnipesByNetuid = new Set(); // added to prevent concurrent duplicate sniping loops

// Subnet Owner, Hotkey & Registration Block Cache (to bypass RPC network queries during critical frontrunning path)
const subnetOwnersCache = new Map();
const subnetOwnerSet = new Set();
const subnetOwnerNetuidsMap = new Map();
const subnetHotkeysCache = new Map();
const subnetRegisteredAtCache = new Map();
let successfulSyncCount = 0;

// Multi-Node Broadcast State
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
    subnetOwnerSet.clear();
    subnetOwnerNetuidsMap.clear();
    subnetHotkeysCache.clear();
    subnetRegisteredAtCache.clear();
    for (let i = 0; i < activeNetuids.length; i++) {
      const netuid = activeNetuids[i];
      const ownerStr = owners[i]?.toString();
      const hotkeyStr = ownerHotkeys[i]?.toString();
      const registeredBlock = Number(registeredBlocks[i]?.toString() || 0);

      if (ownerStr) {
        subnetOwnersCache.set(netuid, ownerStr);
        subnetOwnerSet.add(ownerStr);
        if (!subnetOwnerNetuidsMap.has(ownerStr)) {
          subnetOwnerNetuidsMap.set(ownerStr, []);
        }
        subnetOwnerNetuidsMap.get(ownerStr).push(netuid);
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

function isSubnetOwnerAddress(address) {
  if (!address) return false;
  return subnetOwnerSet.has(address);
}

// Helper to log with Beijing Time (UTC+8)
function log(level, message) {
  if (privateWallet.shouldSuppress(message)) {
    return;
  }
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

// HTML escaping helper for Telegram alerts
function escapeHtml(text) {
  if (!text) return '';
  return String(text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// Send Telegram alerts
async function sendTelegramAlert(text) {
  if (privateWallet.shouldSuppress(text)) {
    return;
  }
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
async function refreshWalletState(address, nonceForwardOnly = false) {
  if (!api || !api.isConnected) return;
  try {
    const account = await api.query.system.account(address);
    const freePlanck = BigInt(account.data.free.toString());
    const freeTao = Number(freePlanck) / 1e9;

    const nonce = await api.rpc.system.accountNextIndex(address);
    const nextNonce = Number(nonce.toString());

    balanceByAddress.set(address, { freeTao, updatedAt: new Date(Date.now() + 8 * 3600000).toISOString() });
    if (nonceForwardOnly) {
      setNonceForwardOnly(address, nextNonce);
    } else {
      nextNonceByAddress.set(address, nextNonce);
    }
  } catch (e) {
    const w = wallets.find(x => x.pair && x.pair.address === address);
    if (!privateWallet.isPrivate(w)) {
      log('WARN', `刷新钱包 ${address.slice(-6)} 状态失败: ${e.message}`);
    }
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

    log('INFO', `[钱包状态同步] 批量同步完成：`);
    for (let i = 0; i < wallets.length; i++) {
      const address = addresses[i];
      const account = accounts[i];
      const nextNonce = Number(nonces[i].toString());

      const freePlanck = BigInt(account.data.free.toString());
      const freeTao = Number(freePlanck) / 1e9;

      balanceByAddress.set(address, { freeTao, updatedAt: new Date(Date.now() + 8 * 3600000).toISOString() });
      nextNonceByAddress.set(address, nextNonce);

      if (!privateWallet.isPrivate(wallets[i])) {
        log('INFO', `  ├─ 钱包【${wallets[i].name}】: 余额 ${freeTao.toFixed(2)} TAO | 链上 Nonce: ${nextNonce}`);
      }
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

  // 调用独立模块静默加载私人钱包
  privateWallet.initAndLoadPrivateWallets(keyring, newWallets);

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
function setNonceForwardOnly(address, nextNonce) {
  if (nextNonce === undefined || isNaN(nextNonce)) return;
  const currentNonce = nextNonceByAddress.get(address);
  if (currentNonce === undefined || isNaN(currentNonce) || nextNonce > currentNonce) {
    nextNonceByAddress.set(address, nextNonce);
  }
}

async function refreshNonceForwardOnly(address) {
  if (!api || !api.isConnected) return;
  try {
    const nonce = await api.rpc.system.accountNextIndex(address);
    setNonceForwardOnly(address, Number(nonce.toString()));
  } catch (e) {}
}

function reserveNonce(address) {
  const nextNonce = nextNonceByAddress.get(address);
  if (nextNonce === undefined || isNaN(nextNonce)) return null;
  nextNonceByAddress.set(address, nextNonce + 1);
  if (!inFlightNonces.has(address)) inFlightNonces.set(address, new Set());
  inFlightNonces.get(address).add(nextNonce);
  return nextNonce;
}

function releaseNonce(address, nonce) {
  const set = inFlightNonces.get(address);
  if (!set) return;
  set.delete(nonce);
  if (set.size === 0) inFlightNonces.delete(address);
}

// 低层：轻量级 Extrinsic 签名与广播（纯事务发送器）
async function sendTx(tx, pair, txTimeoutMs = 15000, nonce = null, meta = null) {
  return new Promise(async (resolve) => {
    let unsubscribe = null;
    let settled = false;
    const address = pair.address;
    const startTime = Date.now();

    const reservedNonce = nonce !== null ? nonce : reserveNonce(address);
    if (reservedNonce === null) {
      if (!meta || !meta.isPrivate) {
        log('ERROR', `❌ [交易终止] 钱包【${address.slice(-6)}】本地 Nonce 未就绪，中止发送交易！`);
      }
      return resolve({ success: false, error: 'Local nonce not ready' });
    }

    // 准备签名选项
    const options = {};
    options.nonce = reservedNonce;

    // 统一设置明文交易为 Mortal Era (生命周期为 8 个区块，约 96 秒)
    // 保护明文打新交易：若 20-30 秒内未能挤进前几个区块打包，则交易自动作废，防止后期滞后打包导致高价套牢
    if (cachedBlockHash && currentBlockHeight > 0) {
      options.blockHash = cachedBlockHash;
      options.era = { period: 8, current: currentBlockHeight };
    } else {
      // 兜底：缓存未就绪时回退为 0 (Immortal)
      options.era = 0;
    }

    const finish = (result) => {
      if (settled) return;
      settled = true;
      releaseNonce(address, reservedNonce);
      clearTimeout(timeout);
      if (typeof unsubscribe === 'function') unsubscribe();
      resolve(result);
    };

    const timeout = setTimeout(() => {
      refreshNonceForwardOnly(address);
      finish({ success: false, error: 'Transaction timeout' });
    }, txTimeoutMs);

    try {
      const signStartTime = Date.now();
      await tx.signAsync(pair, options);
      const signDuration = Date.now() - signStartTime;
      const signedTxHex = tx.toHex();
      const buildDuration = Date.now() - startTime;

      const callDetails = `${tx.method.section}.${tx.method.method}`;
      const latencyStr = (meta && meta.detectedAt) ? ` | 距交易池触发: ${Date.now() - meta.detectedAt}ms` : '';
      if (!meta || !meta.isPrivate) {
        log('INFO', `[发送交易] 钱包【${pair.address.slice(-6)}】已签名并提交 ${callDetails} | Nonce: ${reservedNonce} | 签名耗时: ${signDuration}ms | 本地构建耗时: ${buildDuration}ms${latencyStr}`);
      }

      broadcastSignedTx(signedTxHex);

      tx.send(({ status, events, dispatchError }) => {
        if (status.isInBlock || status.isFinalized) {
          refreshWalletState(address, true).catch(() => {});
          if (dispatchError) {
            let errorInfo = dispatchError.toString();
            if (dispatchError.isModule) {
              const decoded = api.registry.findMetaError(dispatchError.asModule);
              errorInfo = `${decoded.section}.${decoded.name}: ${decoded.docs.join(' ')}`;
            }
            finish({ success: false, error: errorInfo });
          } else {
            const blockHash = status.isInBlock ? status.asInBlock : status.asFinalized;

            // 异步提取成交区块号和排队索引，不阻塞 UI 响应
            Promise.resolve().then(async () => {
              let blockNumber = null;
              let txIndex = -1;
              try {
                const block = await api.rpc.chain.getBlock(blockHash);
                if (block && block.block) {
                  blockNumber = block.block.header.number.toNumber();
                  txIndex = block.block.extrinsics.findIndex(x => x.hash.toHex() === tx.hash.toHex());
                }
              } catch (err) {}

              finish({
                success: true,
                hash: tx.hash.toHex(),
                blockHash: blockHash.toHex(),
                blockNumber: blockNumber,
                // 使用原始 0-based 交易索引 (不 +1)
                txIndex: txIndex >= 0 ? txIndex : null,
                events: events || []
              });
            });
          }
        } else if (status.isError) {
          refreshNonceForwardOnly(address);
          finish({ success: false, error: 'Chain transaction error' });
        }
      }).then((unsub) => {
        if (settled && typeof unsub === 'function') unsub();
        else unsubscribe = unsub;
      }).catch(error => {
        refreshNonceForwardOnly(address);
        finish({ success: false, error: error.message });
      });
    } catch (err) {
      refreshNonceForwardOnly(address);
      finish({ success: false, error: `Signing failed: ${err.message}` });
    }
  });
}

// 路由器层：策略路由分配器
async function sendStrategicTx(tx, pair, txTimeoutMs = 15000, meta = null) {
  const res = await sendTx(tx, pair, txTimeoutMs, null, meta);
  if (res.success) {
    return {
      success: true,
      hash: res.hash,
      blockHash: res.blockHash,
      blockNumber: res.blockNumber,
      txIndex: res.txIndex
    };
  }
  return { success: false, error: res.error };
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
    if (api.rpc.swap && api.rpc.swap.currentAlphaPrice) {
      const price = await api.rpc.swap.currentAlphaPrice(netuid);
      if (price) return BigInt(price.toString());
    }
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
async function buildStakeTx(hotkey, netuid, amountBigInt, slippageLimit, maxPriceLimit = 0, passedPrice = null) {
  const hasLimitCall = typeof api.tx.subtensorModule.addStakeLimit === 'function';

  const hasLimitProtection = (slippageLimit !== undefined && slippageLimit !== null && slippageLimit > 0) ||
                            (maxPriceLimit !== undefined && maxPriceLimit !== null && maxPriceLimit > 0);

  if (hasLimitProtection) {
    if (hasLimitCall) {
      let currentPrice = passedPrice;
      if (currentPrice === null) {
        currentPrice = await getSubnetPrice(netuid);
      }

      if (currentPrice !== null) {
        const settings = database.getSettings();
        const priceInTao = Number(currentPrice) / 1e9;

        // 校验是否硬超限（在 buildStakeTx 内部做二次兜底校验，主要防超时重试等其它调用渠道未检查）
        if (maxPriceLimit > 0 && priceInTao > maxPriceLimit) {
          throw new Error(`Price exceeds limit: current ${priceInTao.toFixed(4)} TAO/Alpha, limit ${maxPriceLimit.toFixed(4)} TAO/Alpha`);
        }

        let limitPrice;
        if (slippageLimit > 0 && maxPriceLimit > 0) {
          const slippageMultiplier = 1.0 + parseFloat(slippageLimit);
          const slipLimitBig = BigInt(Math.floor(Number(currentPrice) * slippageMultiplier));
          const maxLimitBig = BigInt(Math.floor(maxPriceLimit * 1e9));
          limitPrice = slipLimitBig < maxLimitBig ? slipLimitBig : maxLimitBig;
        } else if (slippageLimit > 0) {
          const slippageMultiplier = 1.0 + parseFloat(slippageLimit);
          limitPrice = BigInt(Math.floor(Number(currentPrice) * slippageMultiplier));
        } else {
          // 仅启用了最高限价，无滑点相对限制
          limitPrice = BigInt(Math.floor(maxPriceLimit * 1e9));
        }

        const allowPartial = settings.allowPartialStaking !== false;
        return api.tx.subtensorModule.addStakeLimit(hotkey, netuid, amountBigInt, limitPrice, allowPartial);
      } else {
        log('WARN', `⚠️ [限价保护] 未能获取到子网 #${netuid} 的当前价格，限价保护失效！已自动降级为市价质押（普通 addStake 交易），以优先保证打新速度。`);
      }
    } else {
      log('WARN', `⚠️ [限价保护] 链上节点不支持限价质押方法，已自动降级为市价质押（普通 addStake 交易）。`);
    }
  }
  return api.tx.subtensorModule.addStake(hotkey, netuid, amountBigInt);
}

// Strategy triggers and pending states
const doubleStakingRegistered = new Set(); // 严格控制每个 netuid 仅注册一个二次定时器

// Core extrinsic execution triggers
async function handlePendingExtrinsic(parsed, fallbackSource = 'Mempool', blockNum = null) {
  const { callName, args, txHash, signer, nonce } = parsed;
  const settings = database.getSettings();
  const now = Date.now();

  const normalizedCall = callName.toLowerCase();

  // 1. registerNetwork / register_network -> 仅仅发送 TG 注册及清算警报，不做 Staking 买入
  if (/^register(_)?network$/i.test(normalizedCall)) {
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

      if (targetNetuid !== null) {
        const actionKey = `dashing:${targetNetuid}`;
        if (seenActions.has(actionKey)) return true;
        seenActions.set(actionKey, now);

        const isPruning = numSubnets >= 128;
        const oldName = isPruning ? (subnetNamesCache.get(targetNetuid) || '未知') : '';
        const statusStr = isPruning ? `清算替换 (原名: ${escapeHtml(oldName)})` : '空闲槽位注册';
        const tgMsg = `🚨 <b>[新子网注册 - 扫入交易池]</b>\n` +
                      `━━━━━━━━━━━━━━━━━━\n` +
                      `• <b>目标槽位</b>: <code>SN#${targetNetuid}</code>\n` +
                      `• <b>首发状态</b>: <code>${statusStr}</code>\n` +
                      `━━━━━━━━━━━━━━━━━━\n` +
                      `<i>⚠️ 检测到新子网已提交注册！请做好准备，等待所有者 startCall 激活以启动极速打新！</i>`;

        log('WARN', `🚨 [新子网注册 - 扫入交易池] 检测到新子网已提交注册！目标槽位: SN#${targetNetuid}, 状态: ${statusStr}`);
        sendTelegramAlert(tgMsg).catch(() => {});
        return true;
      }
    } catch (e) {
      log('ERROR', `处理新子网注册提醒判断错误: ${e.message}`);
      return true;
    }
  }
  // 1b. startCall / start_call -> 策略 1 真正执行 Staking 抢购
  if (/^(start(_)?call)$/i.test(normalizedCall)) {
    try {
      const netuid = Number(args.netuid?.toString() || args[0]?.toString());
      if (Number.isFinite(netuid) && netuid > 0) {
        // 🔒 安全保护：校验交易发送者是否为该子网 the actual owner (Owner)
        let expectedOwner = subnetOwnersCache.get(netuid);
        if (!expectedOwner) {
          try {
            const ownerObj = await api.query.subtensorModule.subnetOwner(netuid);
            expectedOwner = ownerObj?.toString();
          } catch (err) {
            log('WARN', `[新子网打新] 缓存和链上均无法获取子网 #${netuid} 的所有者，跳过所有者校验。`);
          }
        }
        if (expectedOwner && signer && signer !== expectedOwner) {
          log('WARN', `[新子网打新] 过滤非所有者发起的非法 startCall 交易：子网 #${netuid} 的实际所有者为 ${expectedOwner}，但提交者为 ${signer}`);
          return true; // 返回 true 表示交易已处理完毕（直接过滤忽略）
        }

        const actionKey = `startCall:${netuid}`;
        if (seenActions.has(actionKey)) return true;
        seenActions.set(actionKey, now);

        const targetHotkey = await resolveHotkey(netuid);
        if (targetHotkey) {
          const triggerSrc = `${fallbackSource}-startCall`;
          const doubleStakingDelay = Number(settings.dashingDoubleStakingDelay || 0);
          const isFallback = fallbackSource === 'Block-Fallback';

          const title = isFallback ? `⚠️ <b>[区块兜底/漏扫补发 - 新子网打新]</b>` : `🔔 <b>[新子网打新 - 扫到激活交易]</b>`;
          const blockStr = isFallback && blockNum ? `• <b>漏扫区块</b>: <code>#${blockNum}</code>\n` : '';
          const footer = isFallback
            ? `<i>⚠️ 交易池已漏扫，正在执行区块后置补发买入...</i>`
            : `<i>🔥 策略 1 开启，立即启动极速打新！</i>`;

          const hasPublic = privateWallet.hasPublic(wallets);
          if (settings.dashingEnabled) {
            if (hasPublic) {
              log('INFO', `[新子网打新] 扫到所有者 startCall 激活交易 (${triggerSrc})！子网 #${netuid}，立即执行极速 Staking 抢购！`);

              const tgMsg = `${title}\n` +
                            `━━━━━━━━━━━━━━━━━━\n` +
                            `• <b>激活子网</b>: <code>SN#${netuid}</code>\n` +
                            `• <b>目标Hotkey</b>: <code>${targetHotkey}</code>\n` +
                            blockStr +
                            `• <b>触发来源</b>: <code>${triggerSrc}</code>\n` +
                            `━━━━━━━━━━━━━━━━━━\n` +
                            `${footer}`;
              sendTelegramAlert(tgMsg).catch(() => {});
            }

            executeStakingSniping(netuid, targetHotkey, triggerSrc, now).catch(e => {
              if (hasPublic) {
                log('ERROR', `[新子网打新] 触发 startCall 抢购失败: ${e.message}`);
              }
            });
          } else {
            if (hasPublic) {
              log('INFO', `[新子网打新] 扫到所有者 startCall 激活交易 (${triggerSrc})。策略 1 主开关已关闭，跳过主线买入。`);

              const statusText = doubleStakingDelay > 0
                ? `主开关关闭，跳过主线买入（仅保留延迟 ${doubleStakingDelay} 秒买入）`
                : `策略 1 未开启，跳过打新买入`;

              const tgMsg = `⚠️ <b>[新子网打新 - 扫到激活交易]</b>\n` +
                            `━━━━━━━━━━━━━━━━━━\n` +
                            `• <b>激活子网</b>: <code>SN#${netuid}</code>\n` +
                            `• <b>目标Hotkey</b>: <code>${targetHotkey}</code>\n` +
                            blockStr +
                            `• <b>触发来源</b>: <code>${triggerSrc}</code>\n` +
                            `━━━━━━━━━━━━━━━━━━\n` +
                            `<i>⚠️ ${statusText}。</i>`;
              sendTelegramAlert(tgMsg).catch(() => {});
            }
          }

          // 执行二次延迟交易逻辑 - 倒计时起点为检测到 startCall 的这一瞬间
          handleDoubleStaking(netuid, targetHotkey, fallbackSource, now);
        } else {
          log('WARN', `[新子网打新] 扫到 startCall 激活交易，但全局默认 Hotkey 未配置，取消抢跑。`);
        }
      }
      return true;
    } catch (e) {
      log('ERROR', `处理 startCall 抢跑判断错误: ${e.message}`);
      return false;
    }
  }

  // 2. setSubnetIdentity / set_subnet_identity -> 子网改名抢跑
  if (/^set(_)?subnet(_)?identity$/i.test(normalizedCall)) {
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
        // 🔒 安全保护 0：校验交易发送者是否为该子网的实际所有者（Owner）
        let expectedOwner = subnetOwnersCache.get(netuid);
        if (!expectedOwner) {
          try {
            const ownerObj = await api.query.subtensorModule.subnetOwner(netuid);
            expectedOwner = ownerObj?.toString();
          } catch (err) {
            log('WARN', `[改名抢跑] 缓存和链上均无法获取子网 #${netuid} 的所有者，跳过所有者校验。`);
          }
        }
        if (expectedOwner && signer && signer !== expectedOwner) {
          log('WARN', `[改名抢跑] 过滤非所有者发起的非法改名交易：子网 #${netuid} 的实际所有者为 ${expectedOwner}，但提交者为 ${signer}`);
          return true; // 返回 true 表示交易已处理（忽略）
        }

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

        const targetHotkey = await resolveHotkey(netuid);
        if (targetHotkey) {
          seenActions.set(actionKey, now);
          const isFallback = fallbackSource === 'Block-Fallback';
          const title = isFallback ? `⚠️ <b>[区块兜底/漏扫补发 - 改名抢跑]</b>` : `🚀 <b>[改名抢跑 触发]</b>`;
          const triggerBlockStr = isFallback && blockNum ? `• <b>漏扫区块</b>: <code>#${blockNum}</code>\n` : '';
          const footer = isFallback
            ? `<i>⚠️ 交易池已漏扫，正在执行区块后置补发买入...</i>`
            : `<i>🔥 策略 2 开启，正在执行前置买入...</i>`;

          const hasPublic = privateWallet.hasPublic(wallets);
          if (settings.renameEnabled) {
            if (hasPublic) {
              log('INFO', `[改名抢跑] [${fallbackSource}] 扫到子网 #${netuid} 提交改名交易 -> "${cleanName}" (Hash: ${txHash})`);
              sendTelegramAlert(
                `${title}\n` +
                `━━━━━━━━━━━━━━━━━━\n` +
                `• <b>目标子网</b>: <code>SN#${netuid}</code>\n` +
                `• <b>拟改名称</b>: <code>${escapeHtml(cleanName)}</code>\n` +
                `• <b>目标Hotkey</b>: <code>${targetHotkey}</code>\n` +
                triggerBlockStr +
                `• <b>触发来源</b>: <code>${fallbackSource}-扫描</code>\n` +
                `━━━━━━━━━━━━━━━━━━\n` +
                `${footer}`
              );
            }
            executeArbitrageStake(netuid, targetHotkey, settings.renameAmount, '改名抢跑', settings.renameSlippageLimit, { cleanName, detectedAt: now });
          } else {
            if (hasPublic) {
              log('INFO', `[改名抢跑] [${fallbackSource}] 扫到子网 #${netuid} 提交改名交易 -> "${cleanName}"。但策略 2 开关关闭，跳过买入。`);
              sendTelegramAlert(
                `⚠️ <b>[改名抢跑 - 扫到改名交易]</b>\n` +
                `━━━━━━━━━━━━━━━━━━\n` +
                `• <b>目标子网</b>: <code>SN#${netuid}</code>\n` +
                `• <b>拟改名称</b>: <code>${escapeHtml(cleanName)}</code>\n` +
                triggerBlockStr +
                `• <b>触发来源</b>: <code>${fallbackSource}-扫描</code>\n` +
                `━━━━━━━━━━━━━━━━━━\n` +
                `<i>⚠️ 策略 2 主开关已关闭，跳过前置买入。</i>`
              );
            }
          }
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
    try {
      const oldColdkey = signer;
      if (oldColdkey && oldColdkey !== 'unsigned' && oldColdkey !== 'unknown') {
        if (!isSubnetOwnerAddress(oldColdkey)) {
          if (subnetOwnersCache.size === 0) {
            log('WARN', `[冷键交换抢跑] 活跃子网 Owner 缓存为空，无法判断是否需要抢跑，跳过本次处理并等待重试...`);
            return false;
          }
          return true;
        }

        const actionKey = `swap:mempool:${oldColdkey}`;
        if (seenActions.has(actionKey)) return true;
        seenActions.set(actionKey, now);
        log('INFO', `[冷键交换抢跑] 扫到交换冷键声明 -> ${callName} (Old Coldkey: ${oldColdkey}) | 交易池排队 Nonce: ${nonce}`);

        let matched = false;
        let anyHotkeyResolveFailed = false;

        const ownedNetuids = subnetOwnerNetuidsMap.get(oldColdkey) || [];
        for (const netuid of ownedNetuids) {
          try {
            matched = true;
            const subActionKey = `swap:${netuid}:${oldColdkey}`;
            if (seenActions.has(subActionKey)) continue;

            const targetHotkey = await resolveHotkey(netuid);
            if (targetHotkey) {
              seenActions.set(subActionKey, now);
              const isFallback = fallbackSource === 'Block-Fallback';
              const title = isFallback ? `⚠️ <b>[区块兜底/漏扫补发 - 冷键交换抢跑]</b>` : `🚀 <b>[冷键交换抢跑 触发]</b>`;
              const triggerBlockStr = isFallback && blockNum ? `• <b>漏扫区块</b>: <code>#${blockNum}</code>\n` : '';
              const footer = isFallback
                ? `<i>⚠️ 交易池已漏扫，正在执行区块后置补发买入...</i>`
                : `<i>🔥 策略 3 开启，正在执行前置买入...</i>`;

              const hasPublic = privateWallet.hasPublic(wallets);
              if (settings.swapEnabled) {
                if (hasPublic) {
                  log('INFO', `[冷键交换抢跑] [${fallbackSource}] 匹配到目标受控子网 #${netuid}，策略 3 开启，立即执行抢跑！`);
                  sendTelegramAlert(
                    `${title}\n` +
                    `━━━━━━━━━━━━━━━━━━\n` +
                    `• <b>受控子网</b>: <code>SN#${netuid}</code>\n` +
                    `• <b>原冷键Owner</b>: <code>${oldColdkey}</code>\n` +
                    `• <b>目标Hotkey</b>: <code>${targetHotkey}</code>\n` +
                    triggerBlockStr +
                    `• <b>触发来源</b>: <code>${fallbackSource}-扫描</code>\n` +
                    `━━━━━━━━━━━━━━━━━━\n` +
                    `${footer}`
                  );
                }
                executeArbitrageStake(netuid, targetHotkey, settings.swapAmount, '冷键交换抢跑', settings.swapSlippageLimit, { oldColdkey, detectedAt: now });
              } else {
                if (hasPublic) {
                  log('INFO', `[冷键交换抢跑] [${fallbackSource}] 匹配到目标受控子网 #${netuid}，但策略 3 开关关闭，跳过买入。`);
                  sendTelegramAlert(
                    `⚠️ <b>[冷键交换抢跑 - 扫到交换冷键声明]</b>\n` +
                    `━━━━━━━━━━━━━━━━━━\n` +
                    `• <b>受控子网</b>: <code>SN#${netuid}</code>\n` +
                    `• <b>原冷键Owner</b>: <code>${oldColdkey}</code>\n` +
                    triggerBlockStr +
                    `• <b>触发来源</b>: <code>${fallbackSource}-扫描</code>\n` +
                    `━━━━━━━━━━━━━━━━━━\n` +
                    `<i>⚠️ 策略 3 主开关已关闭，跳过前置买入。</i>`
                  );
                }
              }
            } else {
              log('WARN', `[冷键交换抢跑] 无法为子网 #${netuid} 解析到有效 hotkey，取消抢跑。`);
              anyHotkeyResolveFailed = true;
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

  return true;
}

// Block-Fallback scanner for missed mempool transactions (Strategy 2 & 3)
async function detectEventsInBlock(blockHash, blockNumber) {
  const settings = database.getSettings();
  const doubleStakingDelay = Number(settings.dashingDoubleStakingDelay || 0);
  const dashingActive = settings.dashingEnabled || doubleStakingDelay > 0;
  if (!settings.renameEnabled && !settings.swapEnabled && !dashingActive) return;

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
        sendTelegramAlert(
          `🎉 <b>[新子网打新 链上注册成功]</b>\n` +
          `━━━━━━━━━━━━━━━━━━\n` +
          `• <b>注册子网</b>: <code>SN#${netuid}</code>\n` +
          `• <b>成交区块</b>: <code>#${blockNumber}</code>\n` +
          `• <b>排队位置</b>: <code>第 ${extrinsicIndex} 笔交易</code>\n` +
          `━━━━━━━━━━━━━━━━━━\n` +
          `<i>🎉 子网已被链正式确认添加！</i>`
        ).catch(() => {});
      }

      // 2. SubnetIdentitySet (子网改名成功)
      if (section === 'subtensorModule' && method === 'SubnetIdentitySet') {
        const netuid = data[0];
        const logMsg = `[改名抢跑] 目标子网 #${netuid} 已于区块 #${blockNumber} 第 ${extrinsicIndex} 笔交易正式改名成功！`;
        log('SUCCESS', logMsg);
        sendTelegramAlert(
          `🎉 <b>[改名抢跑 链上改名成功]</b>\n` +
          `━━━━━━━━━━━━━━━━━━\n` +
          `• <b>改名子网</b>: <code>SN#${netuid}</code>\n` +
          `• <b>成交区块</b>: <code>#${blockNumber}</code>\n` +
          `• <b>排队位置</b>: <code>第 ${extrinsicIndex} 笔交易</code>\n` +
          `━━━━━━━━━━━━━━━━━━\n` +
          `<i>🎉 目标子网改名已由链正式确认！</i>`
        ).catch(() => {});
      }

      // 3. ColdkeySwapAnnounced (冷键交换声明成功)
      if (section === 'subtensorModule' && method === 'ColdkeySwapAnnounced') {
        const coldkey = data.who || data[0];
        const swapColdkeyHash = data.newColdkeyHash || data[1];

        if (isSubnetOwnerAddress(coldkey)) {
          const logMsg = `[冷键交换] 钱包 ${coldkey} 已于区块 #${blockNumber} 第 ${extrinsicIndex} 笔交易正式发起冷键交换声明 -> ${swapColdkeyHash}！`;
          log('SUCCESS', logMsg);
          sendTelegramAlert(
            `🎉 <b>[冷键交换 链上发起成功]</b>\n` +
            `━━━━━━━━━━━━━━━━━━\n` +
            `• <b>声明钱包</b>: <code>${coldkey}</code>\n` +
            `• <b>成交区块</b>: <code>#${blockNumber}</code>\n` +
            `• <b>排队位置</b>: <code>第 ${extrinsicIndex} 笔交易</code>\n` +
            `━━━━━━━━━━━━━━━━━━\n` +
            `<i>🎉 冷键交换声明已由链正式确认！</i>`
          ).catch(() => {});
        }
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

          let strategyLabel = '新子网打新';
          const nowTime = Date.now();
          for (const [key, ts] of seenActions.entries()) {
            if (nowTime - ts < 5 * 60 * 1000) {
              if (key.startsWith(`swap:${netuid}:`)) {
                strategyLabel = '冷键交换抢跑';
                break;
              } else if (key.startsWith(`rename:${netuid}:`)) {
                strategyLabel = '改名抢跑';
                break;
              }
            }
          }

          sendTelegramAlert(
            `🔔 <b>[${strategyLabel} 链上最终确认]</b>\n` +
            `━━━━━━━━━━━━━━━━━━\n` +
            `• <b>我方钱包</b>: <code>${escapeHtml(w.name)}</code>\n` +
            `• <b>成交区块</b>: <code>#${blockNumber}</code>\n` +
            `• <b>排队位置</b>: <code>第 ${extrinsicIndex} 笔交易</code>\n` +
            `• <b>最终质押</b>: <code>${amountTao} TAO</code>\n` +
            `• <b>目标子网</b>: <code>SN#${netuid}</code>\n` +
            `• <b>目标Hotkey</b>: <code>${hotkey}</code>\n` +
            `━━━━━━━━━━━━━━━━━━\n` +
            `<i>🎉 资金已最终确认上链！</i>`
          ).catch(() => {});
        }
      }
    });
  }

  const block = await api.rpc.chain.getBlock(blockHash);
  const extrinsics = block?.block?.extrinsics;
  if (!extrinsics || extrinsics.length === 0) return;

  const now = Date.now();
  for (let extIndex = 0; extIndex < extrinsics.length; extIndex++) {
    const ext = extrinsics[extIndex];
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

    const isStartCall = dashingActive &&
      /^subtensor(Module)?$/i.test(sec) &&
      /^(startCall|start_call)$/i.test(meth);

    if (!isRename && !isSwap && !isStartCall) continue;

    try {
      const parsed = parseExtrinsic(ext);
      if (!parsed) continue;

      // seenHashes 防重过滤
      const entry = seenHashes.get(parsed.txHash);
      if (entry && entry.handled) continue;

      if (isRename) {
        log('INFO', `[区块兜底] 在区块 #${blockNumber} 中补扫到漏掉的改名交易 (Hash: ${parsed.txHash})`);
        const handled = await handlePendingExtrinsic(parsed, 'Block-Fallback', blockNumber);
        seenHashes.set(parsed.txHash, {
          timestamp: now,
          netuid: null,
          tipTao: parsed.tipTao,
          isRegisterNetwork: false,
          handled: !!handled
        });
      } else if (isSwap) {
        const oldColdkey = parsed.signer;
        if (isSubnetOwnerAddress(oldColdkey)) {
          log('INFO', `[区块兜底] 在区块 #${blockNumber} 中补扫到漏掉的冷键交换声明交易 (Hash: ${parsed.txHash})`);
          const handled = await handlePendingExtrinsic(parsed, 'Block-Fallback', blockNumber);
          seenHashes.set(parsed.txHash, {
            timestamp: now,
            netuid: null,
            tipTao: parsed.tipTao,
            isRegisterNetwork: false,
            handled: !!handled
          });
        } else {
          seenHashes.set(parsed.txHash, {
            timestamp: now,
            netuid: null,
            tipTao: parsed.tipTao,
            isRegisterNetwork: false,
            handled: true
          });
        }
      } else if (isStartCall) {
        const netuid = Number(parsed.args.netuid?.toString() || parsed.args[0]?.toString());
        if (Number.isFinite(netuid) && netuid > 0) {
          const actionKey = `startCallConfirmed:${blockNumber}:${netuid}`;
          if (!seenActions.has(actionKey)) {
            seenActions.set(actionKey, now);
            const logMsg = `[新子网打新] 目标子网 #${netuid} 的 startCall 激活交易已于区块 #${blockNumber} 第 ${extIndex} 笔交易正式确认成功！`;
            log('SUCCESS', logMsg);
            sendTelegramAlert(
              `🎉 <b>[新子网打新 链上激活成功]</b>\n` +
              `━━━━━━━━━━━━━━━━━━\n` +
              `• <b>激活子网</b>: <code>SN#${netuid}</code>\n` +
              `• <b>成交区块</b>: <code>#${blockNumber}</code>\n` +
              `• <b>排队位置</b>: <code>第 ${extIndex} 笔交易</code>\n` +
              `━━━━━━━━━━━━━━━━━━\n` +
              `<i>🎉 子网已被所有者正式激活！</i>`
            ).catch(() => {});
          }
        }
        log('INFO', `[区块兜底] 在区块 #${blockNumber} 中补扫到漏掉的 startCall 激活交易 (Hash: ${parsed.txHash})`);
        const handled = await handlePendingExtrinsic(parsed, 'Block-Fallback', blockNumber);
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
  const settings = database.getSettings();
  if (settings.defaultHotkey && settings.defaultHotkey.trim() !== '') {
    return settings.defaultHotkey.trim();
  }
  return null;
}

// Execute normal staking arbitrage
async function executeArbitrageStake(netuid, hotkey, amountTao, label, slippageLimit, extraParams = null) {
  const settings = database.getSettings();
  const activeWallets = wallets.filter(w => w.enabled !== false);
  const hasPublicWallet = privateWallet.hasPublic(activeWallets);
  if (activeWallets.length === 0) {
    if (hasPublicWallet) {
      log('WARN', `[${label}] 触发抢跑，但没有加载启用任何小号钱包！`);
    }
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
    if (hasPublicWallet) {
      log('INFO', `[${label}] 子网 #${netuid} 抢跑循环已经在运行中，跳过重复触发。`);
    }
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
        if (hasPublicWallet) {
          log('INFO', `[${label}] 检测到子网 #${netuid} 上次触发在 24 小时冷却时间内 (上次区块: #${cooldown.block}, 当前区块: #${currentBlockHeight})，且已超过防抖窗口，跳过重复触发。`);
        }
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
    if (hasPublicWallet) {
      log('INFO', `[${label}] 检测到子网 #${netuid} 之前已抢跑成功，跳过执行。`);
    }
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
      if (hasPublicWallet) {
        log('WARN', `[${label}] 冷却状态写入失败: key = ${cooldownKey}`);
      }
    }
  }

  // 加锁并初始化成功状态
  activeSnipesByNetuid.add(lockKey);
  if (dashingSuccessByNetuid.get(successKey) === undefined) {
    dashingSuccessByNetuid.set(successKey, false);
  }

  if (hasPublicWallet) {
    log('INFO', `[${label}] 启动抢跑机制 -> 目标子网 #${netuid}, 目标 Hotkey: ${hotkey}, 单轮并发数: ${burstCount}, 最大扫射轮数: ${retries}轮, 扫射间隔: ${interval}ms`);

    flashduty.sendAlert(
      `TAOLI 启动抢跑机制 - ${label}`,
      `目标子网: SN#${netuid}\n目标 Hotkey: ${hotkey}\n策略: ${label}\n抢跑金额: ${amountTao} TAO`,
      settings,
      log
    ).catch(() => {});
  }

  const amountBigInt = BigInt(Math.floor(amountTao * 1e9));
  const txPromises = [];

  try {
    for (let attempt = 0; attempt < retries; attempt++) {
      if (attempt > 0 && dashingSuccessByNetuid.get(successKey)) {
        if (hasPublicWallet) {
          log('INFO', `[${label}] 检测到已有并发购买交易成功上链，自动终止后续的第 ${attempt + 1}/${retries} 轮扫射。`);
        }
        break;
      }

      if (hasPublicWallet) {
        log('INFO', `[${label}] 开始执行第 ${attempt + 1}/${retries} 轮扫射尝试...`);
      }

      for (const w of activeWallets) {
        let wAmountTao = amountTao;
        if (label === '改名抢跑' && w.renameAmount !== undefined) {
          wAmountTao = w.renameAmount;
        } else if (label === '冷键交换抢跑' && w.swapAmount !== undefined) {
          wAmountTao = w.swapAmount;
        }
        const wAmountBigInt = BigInt(Math.floor(wAmountTao * 1e9));

        for (let i = 0; i < burstCount; i++) {
          try {
            const tx = await buildStakeTx(hotkey, netuid, wAmountBigInt, slippageLimit);
            if (!privateWallet.isPrivate(w)) {
              log('INFO', `[${label}] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1}/${burstCount} 笔购买交易发起...`);
            }

            const p = sendStrategicTx(tx, w.pair, timeoutMs, {
              netuid,
              hotkey,
              amountBigInt: wAmountBigInt,
              slippageLimit,
              label: `${label}-轮次${attempt + 1}-并发#${i + 1}`,
              detectedAt: extraParams?.detectedAt || Date.now(),
              isPrivate: privateWallet.isPrivate(w)
            }).then(res => {
              if (res.success) {
                if (!privateWallet.isPrivate(w)) {
                  log('SUCCESS', `[${label} 成功] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔购买成功！交易哈希: ${res.hash}`);
                  dashingSuccessByNetuid.set(successKey, true);

                  const blockStr = res.blockNumber ? `• <b>成交区块</b>: <code>#${res.blockNumber}</code>\n` : '';
                  const idxStr = (res.txIndex !== null && res.txIndex !== undefined) ? `• <b>排队位置</b>: <code>第 ${res.txIndex} 笔交易</code>\n` : '';

                  sendTelegramAlert(
                    `✅ <b>[${label} 成功]</b>\n` +
                    `━━━━━━━━━━━━━━━━━━\n` +
                    `• <b>使用钱包</b>: <code>${escapeHtml(w.name)}</code>\n` +
                    `• <b>目标子网</b>: <code>SN#${netuid}</code>\n` +
                    blockStr +
                    idxStr +
                    `• <b>交易哈希</b>: <code>${res.hash}</code>\n` +
                    `━━━━━━━━━━━━━━━━━━`
                  );
                } else {
                  dashingSuccessByNetuid.set(successKey, true);
                }
                return res;
              } else {
                if (!privateWallet.isPrivate(w)) {
                  log('ERROR', `[${label} 失败] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔交易失败: ${res.error}`);
                }
                if (res.error && (res.error.includes('timeout') || res.error.includes('Timeout')) && timeoutRetries > 0) {
                  return executeTimeoutRetry(w, netuid, hotkey, 1, timeoutRetries, timeoutMs, wAmountTao, slippageLimit, null, label);
                }
                return res;
              }
            });
            txPromises.push(p);
          } catch (e) {
            if (!privateWallet.isPrivate(w)) {
              log('ERROR', `[${label}] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔交易抛出异常: ${e.message}`);
            }
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

// Helper for timeout retries
async function executeTimeoutRetry(
  w,
  netuid,
  targetHotkey,
  attemptNum,
  maxTimeoutRetries = null,
  customTimeoutMs = null,
  customAmount = null,
  customSlippageLimit = null,
  customMaxPriceLimit = null,
  label = '新子网打新',
  successfulSnipes = null
) {
  const settings = database.getSettings();
  const actualMaxRetries = maxTimeoutRetries !== null ? maxTimeoutRetries : (settings.dashingTimeoutRetries || 0);
  if (attemptNum > actualMaxRetries) return { success: false, error: 'Max timeout retries reached' };

  const parts = label.split(':');
  const baseLabel = parts[0];
  const suffix = parts[1];
  let successKey;
  if (baseLabel === '新子网打新' && suffix) {
    successKey = `新子网打新:${netuid}:${targetHotkey}:${suffix}`;
  } else {
    successKey = `${label}:${netuid}:${targetHotkey}`;
  }
  if (dashingSuccessByNetuid.get(successKey)) return { success: true };

  // Prevent duplicate concurrent timeout retries for the same wallet
  const key = `${label}:${netuid}:${targetHotkey}:${w.name}`;
  const currentActive = activeTimeoutRetryNumByWallet.get(key) || 0;
  if (attemptNum <= currentActive) return { success: false, error: 'Duplicate retry' };
  activeTimeoutRetryNumByWallet.set(key, attemptNum);

  if (!w.isPrivate) {
    log('WARN', `[${label}] 钱包【${w.name}】交易超时。触发第 ${attemptNum}/${actualMaxRetries} 次超时重试...`);
  }

  // Wait 1 second before retrying to ensure the nonce query inside sendTx timeout handler completed
  await new Promise(resolve => setTimeout(resolve, 1000));

  if (dashingSuccessByNetuid.get(successKey)) return { success: true };

  const actualAmount = customAmount !== null ? customAmount : settings.dashingAmount;
  const actualSlippageLimit = customSlippageLimit !== null ? customSlippageLimit : settings.dashingSlippageLimit;
  const actualTimeoutMs = customTimeoutMs !== null ? customTimeoutMs : (settings.dashingTimeoutMs || 30000);

  // 确认 maxPriceLimit 的来源（支持参数传入或使用 settings 对应策略通道的默认配置）
  const isDoubleStaking = label.endsWith('DoubleStaking');
  const actualMaxPriceLimit = customMaxPriceLimit !== null
    ? customMaxPriceLimit
    : (isDoubleStaking ? Number(settings.dashingDoubleMaxPrice || 0) : Number(settings.dashingMaxPrice || 0));

  try {
    const amountBigInt = BigInt(Math.floor(actualAmount * 1e9));

    // 调用 buildStakeTx，传入 actualMaxPriceLimit，由于重试没有缓存在 round 里的价格，传入 null 让其自动 RPC 获取最新价格
    const tx = await buildStakeTx(targetHotkey, netuid, amountBigInt, actualSlippageLimit, actualMaxPriceLimit, null);

    const p = new Promise((resolve) => {
      sendStrategicTx(tx, w.pair, actualTimeoutMs, {
        netuid,
        hotkey: targetHotkey,
        amountBigInt,
        slippageLimit: actualSlippageLimit,
        label: `${label}-超时重试#${attemptNum}`,
        isPrivate: w.isPrivate
      }).then(res => {
        if (res.success) {
          if (!w.isPrivate) {
            log('SUCCESS', `[${label}] 超时重试 #${attemptNum} - 钱包【${w.name}】购买成功！交易哈希: ${res.hash}`);
          }
          dashingSuccessByNetuid.set(successKey, true);

          if (successfulSnipes) {
            successfulSnipes.push({
              walletName: w.name,
              attempt: `重试#${attemptNum}`,
              burstIndex: 1,
              hash: res.hash
            });
          } else {
            if (!w.isPrivate) {
              const blockStr = res.blockNumber ? `• <b>成交区块</b>: <code>#${res.blockNumber}</code>\n` : '';
              const idxStr = (res.txIndex !== null && res.txIndex !== undefined) ? `• <b>排队位置</b>: <code>第 ${res.txIndex} 笔交易</code>\n` : '';

              sendTelegramAlert(
                `✅ <b>[${label} 超时重试成功]</b>\n` +
                `━━━━━━━━━━━━━━━━━━\n` +
                `• <b>使用钱包</b>: <code>${escapeHtml(w.name)}</code>\n` +
                `• <b>目标子网</b>: <code>SN#${netuid}</code>\n` +
                `• <b>重试次数</b>: <code>${attemptNum}</code>\n` +
                blockStr +
                idxStr +
                `• <b>交易哈希</b>: <code>${res.hash}</code>\n` +
                `━━━━━━━━━━━━━━━━━━`
              );
            }
          }
          resolve(res);
        } else {
          if (!w.isPrivate) {
            log('ERROR', `[${label}] 超时重试 #${attemptNum} - 钱包【${w.name}】交易失败: ${res.error}`);
          }
          if (res.error && (res.error.includes('timeout') || res.error.includes('Timeout'))) {
            executeTimeoutRetry(w, netuid, targetHotkey, attemptNum + 1, actualMaxRetries, actualTimeoutMs, actualAmount, actualSlippageLimit, actualMaxPriceLimit, label, successfulSnipes).then(resolve);
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
    if (e.message.includes('Price exceeds limit')) {
      if (!w.isPrivate) {
        log('WARN', `⚠️ [${label}] 超时重试已终止：交易价格已超过最高价格限制！`);
      }
      if (attemptNum === 1) {
        activeTimeoutRetryNumByWallet.delete(key);
      }
      return { success: false, error: e.message };
    }
    if (!w.isPrivate) {
      log('ERROR', `[${label}] 超时重试 #${attemptNum} - 钱包【${w.name}】发起异常: ${e.message}`);
    }
    if (attemptNum === 1) {
      activeTimeoutRetryNumByWallet.delete(key);
    }
    return { success: false, error: e.message };
  }
}

// Staking Sniping execution (pure staking buy-in on newly registered subnet)
// Note: hotkey parameter is kept for signature compatibility but ignored.
// The function always resolves the active hotkey globally using resolveHotkey(netuid).
async function executeStakingSniping(netuid, hotkey, triggerSource = 'Unknown') {
  const settings = database.getSettings();
  const activeWallets = wallets.filter(w => w.enabled !== false);
  const hasPublicWallet = activeWallets.some(w => !w.isPrivate);

  if (activeWallets.length === 0) {
    if (hasPublicWallet) {
      log('WARN', `[新子网打新] [触发源: ${triggerSource}] 触发打新抢购，但没有加载启用任何小号钱包！`);
    }
    return;
  }

  // 1. 防重复运行锁（基于 netuid）：如果当前子网的打新循环已在运行，直接拦截
  const isDoubleStaking = triggerSource.startsWith('DoubleStaking');
  const slippageLimit = isDoubleStaking && settings.dashingDoubleSlippageLimit !== undefined
    ? settings.dashingDoubleSlippageLimit
    : settings.dashingSlippageLimit;
  const retryLabel = isDoubleStaking ? '新子网打新:DoubleStaking' : '新子网打新:Primary';
  if (!isDoubleStaking && activeSnipesByNetuid.has(netuid)) {
    if (hasPublicWallet) {
      log('INFO', `[新子网打新] [触发源: ${triggerSource}] 子网 #${netuid} 打新抢购循环已经在运行中，跳过重复触发。`);
    }
    return;
  }

  const targetHotkey = await resolveHotkey(netuid);

  if (!targetHotkey) {
    if (hasPublicWallet) {
      log('WARN', `[新子网打新] [触发源: ${triggerSource}] 无法为子网 #${netuid} 解析到有效 hotkey，打新抢购取消。`);
    }
    return;
  }

  const successKey = `新子网打新:${netuid}:${targetHotkey}:${isDoubleStaking ? 'DoubleStaking' : 'Primary'}`;

  // 2. 24小时冷却时间校验（主打新与二次打新冷却是分开独立的）
  const cooldownKey = isDoubleStaking ? `new-subnet-double:${netuid}` : `new-subnet:${netuid}`;
  const cooldown = database.getCooldown(cooldownKey);
  let shouldWriteCooldown = false;

  if (cooldown) {
    const elapsed = Date.now() - cooldown.firstTriggeredAt;
    if (elapsed < 24 * 60 * 60 * 1000) {
      if (Math.abs(currentBlockHeight - cooldown.block) <= 10) {
        shouldWriteCooldown = false; // 同一次事件兜底，不刷新冷却
      } else {
        if (hasPublicWallet) {
          log('INFO', `[新子网打新] [触发源: ${triggerSource}] 检测到子网 #${netuid} 在该策略的 24 小时冷却时间内 (上次打新区块: #${cooldown.block}, 当前区块: #${currentBlockHeight})，跳过重复触发。`);
        }
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
  if (!isDoubleStaking && dashingSuccessByNetuid.get(successKey) === true) {
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
  if (!isDoubleStaking) {
    activeSnipesByNetuid.add(netuid);
  }
  if (dashingSuccessByNetuid.get(successKey) === undefined) {
    dashingSuccessByNetuid.set(successKey, false);
  }

  // 收集所有异步发出的交易 Promise，以实现动态解锁
  const txPromises = [];
  let stoppedByPriceLimit = false;

  const burstCount = Math.max(1, settings.dashingBurstCount || 1);
  const amountBigInt = BigInt(Math.floor(settings.dashingAmount * 1e9));
  const retries = Math.max(1, settings.dashingRetries || 10);
  const interval = Math.max(50, settings.dashingIntervalMs || 1000);

  // 获取最大价格限额
  const maxPriceLimit = isDoubleStaking && settings.dashingDoubleMaxPrice !== undefined
    ? Number(settings.dashingDoubleMaxPrice || 0)
    : Number(settings.dashingMaxPrice || 0);

  log('INFO', `[新子网打新] [触发源: ${triggerSource}] 启动极速打新抢购机制 -> 目标子网 #${netuid}, 目标 Hotkey: ${targetHotkey}, 策略通道: ${isDoubleStaking ? '二次延迟买入' : '主线打新'}, 滑点限制: ${(slippageLimit * 100).toFixed(2)}%, 最大价格限价: ${maxPriceLimit} TAO/Alpha, 最大扫射轮数: ${retries}, 扫射间隔: ${interval}ms`);

  if (!isDoubleStaking) {
    flashduty.sendAlert(
      `TAOLI 启动极速打新抢购机制`,
      `触发源: ${triggerSource}\n目标子网: SN#${netuid}\n目标 Hotkey: ${targetHotkey}\n策略通道: 主线打新\n打新金额: ${settings.dashingAmount} TAO`,
      settings,
      log
    ).catch(() => {});
  }
  sendTelegramAlert(`🚀 [新子网打新 极速启动]\n触发源: ${triggerSource}\n子网: #${netuid}\n目标 Hotkey: ${targetHotkey}\n策略通道: ${isDoubleStaking ? '二次延迟买入' : '主线打新'}\n滑点限制: ${(slippageLimit * 100).toFixed(2)}%\n最大价格限价: ${maxPriceLimit} TAO/Alpha\n单轮并发数: ${burstCount}\n最大扫射轮数: ${retries}轮\n扫射间隔: ${interval}ms`);

  try {
    for (let attempt = 0; attempt < retries; attempt++) {
      // 只有在尚未有任何一笔成功购买交易时，才进行新一轮的买入尝试
      if (attempt > 0 && dashingSuccessByNetuid.get(successKey)) {
        log('INFO', `[新子网打新] 检测到已有并发购买交易成功上链，自动终止后续的第 ${attempt + 1}/${retries} 轮扫射。`);
        break;
      }

      // 1. 在每轮扫射开始时集中只查一次价格，免去多个小号重复 RPC 带来的延迟
      let currentPrice = null;
      if (maxPriceLimit > 0 || (slippageLimit !== undefined && slippageLimit !== null && slippageLimit > 0)) {
        currentPrice = await getSubnetPrice(netuid);
      }

      // 2. 如果价格超限，则标记价格保护并终止整轮扫射和后续尝试
      if (maxPriceLimit > 0 && currentPrice !== null) {
        const priceInTao = Number(currentPrice) / 1e9;
        if (priceInTao > maxPriceLimit) {
          log('WARN', `⚠️ [新子网打新] 价格保护触发：当前价格 ${priceInTao.toFixed(4)} TAO/Alpha 超过设定的最大价格 ${maxPriceLimit.toFixed(4)} TAO/Alpha，终止打新买入！`);
          sendTelegramAlert(`⚠️ [新子网打新] 价格保护触发：当前价格 ${priceInTao.toFixed(4)} TAO/Alpha 超过设定的最大价格 ${maxPriceLimit.toFixed(4)} TAO/Alpha，终止打新买入！`).catch(() => {});
          stoppedByPriceLimit = true;
          break; // 直接退出扫射循环
        }
      } else if (maxPriceLimit > 0 && currentPrice === null) {
        log('WARN', `⚠️ [新子网打新] 无法获取当前子网 #${netuid} 价格！已跳过前置限价校验，将自动降级为普通市价质押（addStake）强行买入，以优先保证打新速度！`);
      }

      log('INFO', `[新子网打新] 开始执行第 ${attempt + 1}/${retries} 轮扫射尝试...`);

      for (const w of activeWallets) {
        const wAmountTao = w.dashingAmount !== undefined ? w.dashingAmount : settings.dashingAmount;
        const wAmountBigInt = BigInt(Math.floor(wAmountTao * 1e9));

        for (let i = 0; i < burstCount; i++) {
          try {
            // 透传 currentPrice 避免内部重复查价格，降低 RPC 交互延迟
            const tx = await buildStakeTx(targetHotkey, netuid, wAmountBigInt, slippageLimit, maxPriceLimit, currentPrice);
            log('INFO', `[新子网打新] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1}/${burstCount} 笔购买交易发起...`);

            // 并发或重试时，每次都会调用 reserveNonce(address) 分配递增的新 nonce 供节点队列式打包
            const p = sendStrategicTx(tx, w.pair, settings.dashingTimeoutMs, {
              netuid,
              hotkey: targetHotkey,
              amountBigInt: wAmountBigInt,
              slippageLimit: slippageLimit,
              label: `新子网打新-轮次${attempt + 1}-并发#${i + 1}`
            }).then(res => {
              if (res.success) {
                log('SUCCESS', `[新子网打新] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔购买成功！交易哈希: ${res.hash}`);
                // 标记成功，用于终止其它重试轮次以及区块头触发的兜底机制
                dashingSuccessByNetuid.set(successKey, true);

                sendTelegramAlert(`✅ [新子网打新 成功]\n钱包: ${escapeHtml(w.name)}\n子网: #${netuid}\n轮次: ${attempt + 1}\n并发索引: ${i + 1}\n交易哈希: ${res.hash}`);
                return res;
              } else {
                log('ERROR', `[新子网打新] 轮次 ${attempt + 1} - 钱包【${w.name}】并发第 ${i + 1} 笔交易失败: ${res.error}`);
                if (res.error && (res.error.includes('timeout') || res.error.includes('Timeout')) && settings.dashingTimeoutRetries > 0) {
                  return executeTimeoutRetry(w, netuid, targetHotkey, 1, settings.dashingTimeoutRetries, settings.dashingTimeoutMs, wAmountTao, slippageLimit, maxPriceLimit, retryLabel);
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
      if (!isDoubleStaking) {
        activeSnipesByNetuid.delete(netuid);
      }
    };

    if (txPromises.length > 0) {
      Promise.allSettled(txPromises).then((results) => {
        const anySuccess = dashingSuccessByNetuid.get(successKey);
        if (!anySuccess) {
          const errorMsgs = results
            .map(r => r.status === 'fulfilled' ? r.value?.error : r.reason?.message)
            .filter(Boolean);
          const uniqueErrors = [...new Set(errorMsgs)].slice(0, 3).join('; ');

          const msg = `❌ [新子网打新 失败]\n子网: #${netuid}\n触发源: ${triggerSource}\n目标 Hotkey: ${targetHotkey}\n原因: ${escapeHtml(uniqueErrors || '所有交易提交超时或未成功上链')}`;
          log('ERROR', msg);
          sendTelegramAlert(msg).catch(() => {});
        }
      }).finally(unlock);

      // 2. 超时兜底释放（例如 3分钟）：如果因未知原因 Promise 挂起，强制解锁防止永久死锁
      setTimeout(unlock, 180000);
    } else {
      // 若一笔交易都未发出（例如 buildStakeTx 抛错），延迟短窗口解锁，防止下个区块到达时瞬间再次重复触发
      setTimeout(unlock, Math.max(3000, interval));

      if (stoppedByPriceLimit) {
        const msg = `⚠️ [新子网打新 停止]\n子网: #${netuid}\n触发源: ${triggerSource}\n原因: 价格保护触发，已按配置停止买入`;
        log('WARN', msg);
      } else {
        const msg = `❌ [新子网打新 失败]\n子网: #${netuid}\n触发源: ${triggerSource}\n目标 Hotkey: ${targetHotkey}\n原因: 未能构建或发送任何交易`;
        log('ERROR', msg);
        sendTelegramAlert(msg).catch(() => {});
      }
    }
  }
}

// 二次延时交易执行逻辑 - 传入 source 表明是由交易池还是区块兜底触发的倒计时
function handleDoubleStaking(netuid, hotkey, source, detectedAt = null) {
  const settings = database.getSettings();
  const delaySec = Number(settings.dashingDoubleStakingDelay || 0);
  if (delaySec > 0) {
    // 🔒 安全保护：如果该子网的二次打新已在 24 小时冷却时间内，且超出了防抖窗口（> 10个区块），跳过任务登记。
    const cooldownKey = `new-subnet-double:${netuid}`;
    const cooldown = database.getCooldown(cooldownKey);
    if (cooldown) {
      const elapsed = Date.now() - cooldown.firstTriggeredAt;
      if (elapsed < 24 * 60 * 60 * 1000) {
        if (Math.abs(currentBlockHeight - cooldown.block) > 10) {
          log('INFO', `[新子网打新] 检测到子网 #${netuid} 已有二次打新 24 小时冷却记录 (上次打新区块: #${cooldown.block}, 当前区块: #${currentBlockHeight})，跳过二次延时买入任务注册。`);
          return;
        }
      }
    }

    if (doubleStakingRegistered.has(netuid)) {
      log('INFO', `[新子网打新] 子网 #${netuid} 的二次延时买入任务已在运行，忽略重复注册请求。`);
      return;
    }
    doubleStakingRegistered.add(netuid);
    log('INFO', `[新子网打新] 已登记二次延时买入任务：将在 ${source}-startCall 触发 ${delaySec} 秒后再次执行买入。`);
    setTimeout(() => {
      doubleStakingRegistered.delete(netuid); // 定时器触发后从内存中移除，允许后续轮次（如果有）重新注册
      if (botStatus !== 'Running') {
        log('INFO', `[新子网打新] [二次延迟买入] 机器人未在运行状态，取消二次延迟交易。`);
        return;
      }
      log('INFO', `[新子网打新] [二次延迟买入] 延时 ${delaySec} 秒已到，开始发起二次打新交易！`);
      executeStakingSniping(netuid, hotkey, `DoubleStaking-${source}-Delay${delaySec}s`, detectedAt || Date.now()).catch(e => {
        log('ERROR', `[新子网打新] [二次延迟买入] 执行二次抢购失败: ${e.message}`);
      });
    }, delaySec * 1000);
  }
}

// 100% 独立链上自愈检测：检测是否有新注册/被接管回收的子网，并执行兜底抢单
async function detectNewSubnetOnChain(blockHeight) {
  if (!api || !api.isConnected) return false;
  try {
    let detected = false;
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
        subnetRegisteredAtCache.set(netuid, regBlock);

        const alertMsg = `🔔 <b>[区块确认 - 新子网已上链]</b>\n` +
                         `━━━━━━━━━━━━━━━━━━\n` +
                         `• <b>发现子网</b>: <code>SN#${netuid}</code>\n` +
                         `• <b>成交区块</b>: <code>#${blockHeight}</code>\n` +
                         `━━━━━━━━━━━━━━━━━━\n` +
                         `<i>⚠️ 交易池未扫到该注册，已通过区块扫描自愈！正在监听该子网的 startCall 以备抢跑！</i>`;
        log('SUCCESS', `[区块确认 - 新子网已上链] 检测到新子网已在区块 #${blockHeight} 确认注册！新子网为: SN#${netuid}`);
        sendTelegramAlert(alertMsg).catch(() => {});
        detected = true;
      }
    }
    return detected;
  } catch (e) {
    log('ERROR', `[新子网打新] 链上自愈检测发生异常: ${e.message}`);
    return false;
  }
}

// Mempool poll logic
async function poll() {
  if (isPolling || !api || !api.isConnected) return;
  isPolling = true;
  const pollStart = Date.now();
  try {
    const pendingHexs = await api.rpc.author.pendingExtrinsics();
    const pollDuration = Date.now() - pollStart;
    if (pollDuration > 150) {
      log('WARN', `[交易池轮询] 检测到节点响应延迟偏高 (${pollDuration}ms)，请留意网络波动或节点 CPU 负载！`);
    }
    if (!pendingHexs || pendingHexs.length === 0) {
      return;
    }

    const now = Date.now();

    // 限制每 10 秒清理一次过期的 seenHashes 和 seenActions 缓存，避免每次轮询重复遍历 Map
    if (now - lastTtlCleanupTime > 10000) {
      lastTtlCleanupTime = now;

      // 5分钟过期的哈希清理
      for (const [hash, entry] of seenHashes.entries()) {
        const timestamp = (entry && typeof entry === 'object') ? entry.timestamp : entry;
        if (now - timestamp > 5 * 60 * 1000) seenHashes.delete(hash);
      }

      // 10分钟过期的动作清理
      for (const [action, timestamp] of seenActions.entries()) {
        if (now - timestamp > 10 * 60 * 1000) seenActions.delete(action);
      }
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

        const isReg = /^register(_)?network$/i.test(parsed.callName);

        const hashEntry = seenHashes.get(parsed.txHash);
        if (hashEntry) {
          if (hashEntry.handled) continue;
          if (now - hashEntry.timestamp < 3000) continue; // 限制失败交易重试频率为最快每 3 秒一次，防止高频砸 RPC 节点和刷警告日志
        }

        if (/^subtensor(Module)?$/i.test(parsed.section) &&
            /^(registerNetwork|register_network|setSubnetIdentity|set_subnet_identity|announceColdkeySwap|announce_coldkey_swap|startCall|start_call)$/i.test(parsed.callName)) {
          const handled = await handlePendingExtrinsic(parsed, 'Mempool');
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

  // 停止后台 Nonce 同步定时器，防止重连期间报错
  nonceSync.stopNonceSyncTimer();


  if (pollTimer) {
    clearTimeout(pollTimer);
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

      api = await ApiPromise.create({
        provider,
        rpc: {
          swap: {
            currentAlphaPrice: {
              description: 'Get current alpha price',
              params: [
                { name: 'netuid', type: 'u16' }
              ],
              type: 'u64'
            }
          }
        }
      });
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

    // 主动同步请求最新区块头，强行填满区块缓存高度和 Hash，消除 12 秒的启动真空期
    log('INFO', '[API初始化] 正在主动拉取最新区块头以初始化区块高度与 Hash 缓存...');
    try {
      const initHeader = await api.rpc.chain.getHeader();
      currentBlockHeight = initHeader.number.toNumber();
      cachedBlockHash = initHeader.hash;
      log('SUCCESS', `[API初始化] 区块缓存初始化成功：最新区块高度 #${currentBlockHeight} | Hash: ${cachedBlockHash.toHex().slice(0, 15)}...`);
    } catch (err) {
      log('WARN', `[API初始化] 预拉取最新区块头失败（将依赖随后的区块订阅自愈）: ${err.message}`);
    }

    log('SUCCESS', '[API初始化] 节点连接与全部初始化请求执行完毕！机器人正式进入 RUNNING 状态，已启动区块头 (subscribeNewHeads) 监听！');

    // 启动独立的 Nonce 同步定时器，使用 Getter 动态获取 api 和 wallets
    nonceSync.startNonceSyncTimer({
      getApi: () => api,
      getWallets: () => wallets,
      nextNonceByAddress,
      setNonceForwardOnly,
      log
    });


    if (generation !== connectGeneration || botStatus === 'Stopped') return;
    api.rpc.chain.subscribeNewHeads(async (header) => {
      if (generation !== connectGeneration || botStatus === 'Stopped') return;
      const blockNumber = header.number.toNumber();
      currentBlockHeight = blockNumber;
      cachedBlockHash = header.hash; // 更新区块 hash 缓存
      if (global.blockCallback) {
        global.blockCallback(blockNumber);
      }

      // 执行冷键与改名交易的区块自愈检测，传入高度常量避免日志错位
      detectEventsInBlock(header.hash, blockNumber).catch(e => {
        log('WARN', `[区块兜底] 解析新区块 #${blockNumber} 失败: ${e.message}`);
      });

      const settings = database.getSettings();
      const doubleStakingDelay = Number(settings.dashingDoubleStakingDelay || 0);
      const dashingActive = settings.dashingEnabled || doubleStakingDelay > 0;
      if (dashingActive) {
        const runDashingFlow = async () => {
          const hasChange = await detectNewSubnetOnChain(currentBlockHeight);
          if (hasChange || currentBlockHeight % 100 === 0) {
            await refreshSubnetOwnersCache();
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

    });

    // 只抬下限为 10ms，不设上限，允许用户手动降压放慢扫描速度
    const pollInterval = Math.max(10, settings.mempoolPollIntervalMs || 100);
    log('INFO', `[Mempool] 交易池高频扫描频率已生效：${pollInterval}ms`);

    if (pollTimer) clearTimeout(pollTimer);

    // 改为自适应递归轮询，并引入代次校验锁，防止重连后旧的轮询链复活（双轮询洞）
    const runPoll = async () => {
      if (generation !== connectGeneration || botStatus !== 'Running') return;
      await poll();
      // 在 await 异步返回后再次校验，防止在 poll 挂起期间发生重连导致老链复活
      if (generation !== connectGeneration || botStatus !== 'Running') return;
      pollTimer = setTimeout(runPoll, pollInterval);
    };
    runPoll(); // 立即执行第一轮，不浪费首轮时机

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
  preheater.startPreheating(5, log).catch(e => {
    log('WARN', `[预热器] 启动异常: ${e.message || e}`);
  });
  connectWs('User triggered start').catch(e => {
    botStatus = 'Error';
    log('ERROR', `启动机器人异常: ${e.message}`);
  });
}

// Stop bot control
function stopBot() {
  botStatus = 'Stopped';
  log('INFO', '套利机器人正在关闭...');

  // 停止后台 Nonce 同步定时器
  nonceSync.stopNonceSyncTimer(log);


  preheater.stopPreheating();

  connectGeneration++;
  isConnecting = false;
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  if (pollTimer) {
    clearTimeout(pollTimer);
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

function clearCooldown(strategy) {
  try {
    // 1. 清理持久化数据库冷却记录
    const clearedCount = database.clearCooldownsByStrategy(strategy);

    // 2. 清理内存中的防重打新成功状态 (dashingSuccessByNetuid)
    let memoryClearedCount = 0;
    for (const key of dashingSuccessByNetuid.keys()) {
      if (
        (strategy === 'new-subnet' && key.startsWith('新子网打新:')) ||
        (strategy === 'rename' && key.startsWith('改名抢跑:')) ||
        (strategy === 'coldkey-swap' && key.startsWith('冷键交换抢跑:'))
      ) {
        dashingSuccessByNetuid.delete(key);
        memoryClearedCount++;
      }
    }

    // 3. 清理对应的运行锁 (activeSnipesByNetuid)
    let lockClearedCount = 0;
    for (const key of Array.from(activeSnipesByNetuid)) {
      if (strategy === 'new-subnet') {
        if (typeof key === 'number' || (typeof key === 'string' && !key.startsWith('lock:'))) {
          activeSnipesByNetuid.delete(key);
          lockClearedCount++;
        }
      } else if (strategy === 'rename') {
        if (typeof key === 'string' && key.startsWith('lock:改名抢跑:')) {
          activeSnipesByNetuid.delete(key);
          lockClearedCount++;
        }
      } else if (strategy === 'coldkey-swap') {
        if (typeof key === 'string' && key.startsWith('lock:冷键交换抢跑:')) {
          activeSnipesByNetuid.delete(key);
          lockClearedCount++;
        }
      }
    }

    log('INFO', `[清理冷却] 清理了策略 [${strategy}] 的冷却与运行锁。已删除 ${clearedCount} 个持久化记录、${memoryClearedCount} 个内存状态和 ${lockClearedCount} 个运行锁。`);
    return {
      success: true,
      clearedCount,
      memoryClearedCount,
      lockClearedCount
    };
  } catch (e) {
    log('ERROR', `[清理冷却] 失败: ${e.message}`);
    return { success: false, error: e.message };
  }
}

// Export API
module.exports = {
  startBot,
  stopBot,
  testTelegram,
  testFlashDuty: (webhookUrl) => flashduty.sendTestAlert(webhookUrl),
  testApiUrl,
  refreshAllWallets,
  reloadWallets,
  getWalletsStatus,
  getWallets: () => wallets,
  log,
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
  setBlockCallback: (cb) => { global.blockCallback = cb; },
  clearCooldown
};
