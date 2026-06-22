const { Keyring } = require('@polkadot/keyring');
const { cryptoWaitReady } = require('@polkadot/util-crypto');

let preheatTimer = null;
let dummyPair = null;

async function getDummyPair() {
  await cryptoWaitReady(); // 确保 WASM 异步加载并初始化完毕，防止降级为纯 JS 加密
  if (!dummyPair) {
    const keyring = new Keyring({ type: 'sr25519' });
    dummyPair = keyring.addFromUri('//TaoliPreheatDummy');
  }
  return dummyPair;
}

// 执行一次模拟签名，强行触发 WASM 初始化与 V8 引擎 JIT 编译
async function runMockSignature() {
  const pair = await getDummyPair();
  const message = Buffer.from('Taoli Warmup ' + Date.now());
  pair.sign(message);
}

async function startPreheating(intervalMin = 5, logFn = null) {
  stopPreheating(); // 清理历史定时器

  // 1. 首轮预热：打印 SUCCESS 日志到控制台与前端面板
  const start = Date.now();
  try {
    await runMockSignature();
    const duration = Date.now() - start;
    const msg = `WASM 密码学签名引擎初始化与首轮预热成功！耗时: ${duration}ms。`;
    console.log(`[Preheater] ${msg}`);
    if (typeof logFn === 'function') {
      logFn('SUCCESS', `[预热器] ${msg}`);
    }
  } catch (e) {
    const errorMsg = `首轮密码学预热失败: ${e.message || e}`;
    console.error(`[Preheater] ${errorMsg}`);
    if (typeof logFn === 'function') {
      logFn('WARN', `[预热器] ${errorMsg}`);
    }
  }

  // 2. 后台心跳保活：仅在失败时打印 WARN 日志，避免刷屏
  if (intervalMin > 0) {
    preheatTimer = setInterval(async () => {
      try {
        await runMockSignature();
      } catch (e) {
        const errorMsg = `后台心跳保活预热失败: ${e.message || e}`;
        console.error(`[Preheater] ${errorMsg}`);
        if (typeof logFn === 'function') {
          logFn('WARN', `[预热器] ${errorMsg}`);
        }
      }
    }, intervalMin * 60 * 1000);

    if (typeof preheatTimer.unref === 'function') {
      preheatTimer.unref();
    }
  }
}

function stopPreheating() {
  if (preheatTimer) {
    clearInterval(preheatTimer);
    preheatTimer = null;
  }
}

module.exports = {
  startPreheating,
  stopPreheating
};
