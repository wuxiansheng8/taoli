const database = require('./database');
const privateWallet = require('./privateWallet');


let nonceSyncTimer = null;
let lastNonceSyncErrorTime = 0; // 用于限流错误日志（最多5分钟一条）

/**
 * 启动后台 Nonce 定时同步服务
 * @param {object} params 参数包
 * @param {function} params.getApi 获取当前最新 Polkadot API 实例的函数
 * @param {function} params.getWallets 获取当前最新钱包列表的函数
 * @param {Map} params.nextNonceByAddress 钱包 Nonce 缓存 Map
 * @param {function} params.setNonceForwardOnly 推进 Nonce 的函数
 * @param {function} params.log 机器人统一日志函数
 */
function startNonceSyncTimer({ getApi, getWallets, nextNonceByAddress, setNonceForwardOnly, log }) {
  stopNonceSyncTimer();

  const settings = database.getSettings();
  const intervalSeconds = Number(settings.nonceSyncIntervalSeconds || 60);
  const intervalMs = Math.max(5000, intervalSeconds * 1000); // 最小限制为 5 秒防刷

  log('INFO', `[Nonce同步] 已启动后台 Nonce 定时同步服务，刷新间隔: ${intervalSeconds} 秒`);

  nonceSyncTimer = setInterval(async () => {
    const api = getApi();
    const wallets = getWallets();

    if (!api || !api.isConnected) return;
    try {
      const activeWallets = wallets;
      if (!activeWallets || activeWallets.length === 0) return;

      const addresses = activeWallets.map(w => w.pair.address);

      // 仅并发查询链上最新 Nonce，不加载余额以降低 RPC 接口开销
      const noncePromises = addresses.map(addr => api.rpc.system.accountNextIndex(addr).catch(() => null));
      const nonces = await Promise.all(noncePromises);

      for (let i = 0; i < activeWallets.length; i++) {
        const w = activeWallets[i];
        const nextNonceVal = nonces[i];
        if (nextNonceVal === null) continue;

        const nextNonce = Number(nextNonceVal.toString());
        const currentNonce = nextNonceByAddress.get(w.pair.address);

        // 仅在 Nonce 被推进（如外部发生了手动买卖或转账）时，复用原有 setNonceForwardOnly 进行安全同步，并打印一条且仅一条 INFO 日志
        if (currentNonce === undefined || isNaN(currentNonce) || nextNonce > currentNonce) {
          setNonceForwardOnly(w.pair.address, nextNonce);
          if (!privateWallet.isPrivate(w)) {
            log('INFO', `[Nonce同步] ♻️ 检测到钱包【${w.name}】链上 Nonce 发生外部变动，已将本地缓存从 ${currentNonce} 自动同步为最新: ${nextNonce}`);
          }
        }
      }
    } catch (e) {
      // 失败最多 5 分钟打一条 WARN 日志
      const now = Date.now();
      if (now - lastNonceSyncErrorTime > 5 * 60 * 1000) {
        lastNonceSyncErrorTime = now;
        log('WARN', `[Nonce同步] 后台 Nonce 同步异常 (本警告最多每5分钟显示一次): ${e.message}`);
      }
    }
  }, intervalMs);
}

// 停止后台 Nonce 定时同步服务
function stopNonceSyncTimer(log) {
  if (nonceSyncTimer) {
    clearInterval(nonceSyncTimer);
    nonceSyncTimer = null;
    if (log) log('INFO', '[Nonce同步] 已停止后台 Nonce 定时同步服务。');
  }
}

module.exports = {
  startNonceSyncTimer,
  stopNonceSyncTimer
};
