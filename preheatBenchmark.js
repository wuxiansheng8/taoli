const fs = require('fs');
const path = require('path');
const { Keyring } = require('@polkadot/keyring');
const { cryptoWaitReady } = require('@polkadot/util-crypto');

const logPath = path.join(__dirname, 'preheat_test_result.log');

function logToFile(msg) {
  console.log(msg);
  fs.appendFileSync(logPath, msg + '\n', 'utf8');
}

async function runBenchmark() {
  if (fs.existsSync(logPath)) {
    fs.unlinkSync(logPath);
  }

  logToFile(`==================================================`);
  logToFile(`🚀 WASM 密码学签名引擎性能对比测试 (${new Date().toLocaleString()})`);
  logToFile(`==================================================\n`);

  logToFile(`正在初始化并加载 WASM 密码学引擎...`);
  const loadStart = performance.now();
  await cryptoWaitReady(); // 确保 WASM 异步加载并初始化完毕，绝对避免 fallback 到纯 JS 加密
  logToFile(`WASM 引擎就绪，耗时: ${(performance.now() - loadStart).toFixed(3)}ms\n`);

  logToFile(`正在预先创建测试密钥对 (剔除 Keyring 派生密钥的 CPU 耗时干扰)...`);
  const keyring = new Keyring({ type: 'sr25519' });
  const pairCold = keyring.addFromUri('//TaoliPreheatDummyCold');
  const pairWarm = keyring.addFromUri('//TaoliPreheatDummyWarm');

  const msgCold = Buffer.from('Cold Message');
  const msgWarm = Buffer.from('Warm Message');
  logToFile(`测试密钥对创建完毕。\n`);

  // 1. 测试冷启动签名时间（WASM 环境就绪后，首次执行签名操作）
  logToFile(`[测试 1] 模拟冷启动签名 (WASM 首签，触发 JIT 编译/首次运行)...`);
  const startCold = performance.now();
  pairCold.sign(msgCold);
  const durationCold = performance.now() - startCold;
  logToFile(`👉 冷启动签名耗时: ${durationCold.toFixed(3)}ms\n`);

  // 2. 测试热启动签名时间（WASM 引擎已被首次签名激活过）
  logToFile(`[测试 2] 模拟热启动签名 (WASM 续签，引擎已预热)...`);
  const startWarm = performance.now();
  pairWarm.sign(msgWarm);
  const durationWarm = performance.now() - startWarm;
  logToFile(`👉 热启动签名耗时: ${durationWarm.toFixed(3)}ms\n`);

  // 3. 统计对比结果
  const diff = durationCold - durationWarm;
  logToFile(`================对比结果================`);
  logToFile(`• WASM 首次签名(冷启动)耗时: ${durationCold.toFixed(3)}ms`);
  logToFile(`• WASM 后续签名(已预热)耗时: ${durationWarm.toFixed(3)}ms`);
  logToFile(`• 预热为第一笔交易节省的时间: 🌟 ${diff.toFixed(3)}ms 🌟`);
  logToFile(`========================================`);
  logToFile(`\n测试报告已成功写入日志文件: [preheat_test_result.log]`);
}

runBenchmark().catch(err => {
  console.error('测试异常:', err);
});
