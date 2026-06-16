# 遥利 (Taoli) 控制台详细日志与 Telegram 推送升级实施计划

本计划专注于实现 **详细控制台日志** 与 **Telegram HTML 推送模板** 的升级。之前已审核并上传到 GitHub 的功能（如下线竞价/RBF、新增 3 个策略独立的 MEV 开关等）不在本计划的修改范围内。

---

## 1. 优化目标与逻辑流程 (Optimization Objectives)

### 1.1 交易成交位置分析 (Block Height & Queue Position Index)
* 在底层 `sendTx` 的成功回调中，通过 `api.rpc.chain.getBlock` 异步获取成交区块数据。
* 遍历区块中的 `extrinsics`，定位我方交易的哈希，并计算出排队位置（数组索引 + 1，即第几笔交易）。
* 返回包含 `blockNumber` 与 `txIndex` 的结果对象。

### 1.2 控制台详细日志 (Detailed Console Logs)
* **批量钱包同步**：在 `refreshAllWallets` 同步成功时，使用树状图形式输出所有钱包的可用余额与 Nonce。
* **交易池延迟预警**：在 `poll` 轮询中测量 RPC 响应时间，若超过 150ms 则输出 `WARN` 日志。
* **MEV Shield 加密指标**：在 `sendMevShieldTx` 加密成功后记录 PQ-KEM 封装耗时、密钥剩余区块有效期及内层交易真实 Hash。
* **抢跑时延统计**：签名提交时，计算并输出从**发现机会 (Mempool 触发时间戳)** 到 **开枪提交交易** 所花费的毫秒数。

### 1.3 Telegram 推送格式规范化 (Telegram HTML Templates)
* 全面使用 HTML 模式，利用 `<b>`、`<code>` 和等宽分割线，在手机端以精细格式展示。
* **新子网打新**：主线极速启动与延时启动分别推送不同标题；扫射结束后将成功交易进行一次性“成功汇总”；区块扫描自愈检测到新子网时，推送兜底警报。
* **改名与冷键交换抢跑**：在交易成交后立即获取并展示区块高度与排队位置（如：`第 12 笔交易`）。
* **三明治套利**：区分检测到大额买入、前置买入失败、Alpha 余额异常（自动防损终止）、后置卖出成功（展示排队位置）和后置卖出失败（严重异常警报）等阶段。

---

## 2. 变更文件与拟修改代码 (Proposed Code Changes)

### 2.1 [MODIFY] [bot.js](file:///C:/Users/moshe/Desktop/taoli/bot.js)

#### 2.1.1 升级 `sendTx` 以计算时延并获取成交区块排队索引
```javascript
// 修改底层 sendTx 以支持 meta 参数并异步提取区块/索引信息
async function sendTx(tx, pair, txTimeoutMs = 15000, tip = 0, nonce = null, meta = null) {
  return new Promise(async (resolve) => {
    let unsubscribe = null;
    let settled = false;
    const address = pair.address;
    const startTime = Date.now();

    const reservedNonce = nonce !== null ? nonce : reserveNonce(address);
    if (reservedNonce === null) {
      log('ERROR', `❌ [交易终止] 钱包【${address.slice(-6)}】本地 Nonce 未就绪，中止发送交易！`);
      return resolve({ success: false, error: 'Local nonce not ready' });
    }

    const options = { era: 0 };
    options.nonce = reservedNonce;
    if (tip > 0) options.tip = BigInt(Math.floor(tip * 1e9));

    const finish = (result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      if (typeof unsubscribe === 'function') unsubscribe();
      resolve(result);
    };

    const timeout = setTimeout(() => {
      api.rpc.system.accountNextIndex(address)
        .then(n => nextNonceByAddress.set(address, Number(n.toString())))
        .catch(() => {});
      finish({ success: false, error: 'Transaction timeout' });
    }, txTimeoutMs);

    try {
      await tx.signAsync(pair, options);
      const signedTxHex = tx.toHex();
      const buildDuration = Date.now() - startTime;

      const callDetails = `${tx.method.section}.${tx.method.method}`;
      const latencyStr = (meta && meta.detectedAt) ? ` | 距交易池触发: ${Date.now() - meta.detectedAt}ms` : '';
      log('INFO', `[发送交易] 钱包【${pair.address.slice(-6)}】已签名并提交 ${callDetails} | Nonce: ${reservedNonce} | Tip: ${tip} TAO | 本地构建耗时: ${buildDuration}ms${latencyStr}`);

      broadcastSignedTx(signedTxHex);

      tx.send(({ status, events, dispatchError }) => {
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
            const blockHash = status.isInBlock ? status.asInBlock : status.asFinalized;
            
            // 异步提取成交区块号 and 排队索引，不阻塞 UI 响应
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
                txIndex: txIndex >= 0 ? txIndex + 1 : null,
                events: events || []
              });
            });
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
```

#### 2.1.2 升级 `sendMevShieldTx` 和 `sendStrategicTx` 以输出加密指标并向下透传
```javascript
// 升级 sendMevShieldTx 记录加密耗时与详细日志
async function sendMevShieldTx(innerTx, pair, txTimeoutMs = 15000, tip = 0, meta = null) {
  const address = pair.address;

  // A. 密钥与 Noble 库检查
  const keysExpired = cachedNextKeyExpiresAt > 0 && currentBlockHeight >= cachedNextKeyExpiresAt;
  const cannotEncrypt = !hasMevShieldPallet || !cachedNextKeyBytes || keysExpired || !ml_kem768;

  if (cannotEncrypt) {
    const reason = !hasMevShieldPallet ? '链上未启用 mevShield 模块' :
                   keysExpired ? `密钥已过期 (当前高度: #${currentBlockHeight}, 失效高度: #${cachedNextKeyExpiresAt})` :
                   '未获取到有效加密公钥或 noble 库未就绪';
    log('WARN', `⚠️ [MEV安全降级] 自动安全降级为明文发送！原因: "${reason}"`);
    return sendTx(innerTx, pair, txTimeoutMs, tip, null, meta);
  }

  // B. 申请双 Nonce
  const doubleNonce = reserveNonce(address, true);
  if (!doubleNonce || doubleNonce.outer === null || doubleNonce.inner === null) {
    log('WARN', `⚠️ [MEV安全降级] 钱包【${address.slice(-6)}】双 Nonce 分配失败，自动降级为明文发送！`);
    return sendTx(innerTx, pair, txTimeoutMs, tip, null, meta);
  }

  const { outer: outerNonce, inner: innerNonce } = doubleNonce;
  let encryptionSuccessful = false;
  let innerSignedHash = null;
  let outerTx = null;
  const pqStartTime = Date.now();

  try {
    // 签名内层交易 (Immortal 签名)
    const innerOptions = { nonce: innerNonce, era: 0 };
    await innerTx.signAsync(pair, innerOptions);
    
    // 【从这里明确导出内层交易签名哈希】
    innerSignedHash = innerTx.hash.toHex();
    const innerTxBytes = innerTx.toU8a();

    // 核心 PQ 加密
    const keyHash = xxhashAsU8a(cachedNextKeyBytes, 128);
    const { cipherText: kemCt, sharedSecret } = ml_kem768.encapsulate(cachedNextKeyBytes);
    const nonceBytes = randomBytes(24);
    const cipher = xchacha20poly1305(sharedSecret, nonceBytes);
    const aeadCt = cipher.encrypt(innerTxBytes);

    const kemLenBuf = Buffer.alloc(2);
    kemLenBuf.writeUInt16LE(kemCt.length);

    const packedCiphertext = Buffer.concat([
      Buffer.from(keyHash),
      kemLenBuf,
      Buffer.from(kemCt),
      Buffer.from(nonceBytes),
      Buffer.from(aeadCt)
    ]);

    const ciphertextHex = '0x' + packedCiphertext.toString('hex');

    // 生成 submitEncrypted 外层交易
    outerTx = api.tx.mevShield.submitEncrypted(ciphertextHex);
    encryptionSuccessful = true;
    
    const encryptDuration = Date.now() - pqStartTime;
    log('INFO', `[MEV Shield 加密] 正在构建加密交易...\n` +
                `  ├─ 密钥有效期: 剩余 ${cachedNextKeyExpiresAt - currentBlockHeight} 个区块 (当前: #${currentBlockHeight} / 失效: #${cachedNextKeyExpiresAt})\n` +
                `  ├─ 内层真实哈希 (Inner): ${innerSignedHash}\n` +
                `  ├─ 密文生成成功: PQ-KEM 封装并加密完成 | 耗时: ${encryptDuration}ms | 密文大小: ${packedCiphertext.length} 字节\n` +
                `  └─ 外层交易已打包 (submitEncrypted)，准备广播...`);
  } catch (err) {
    log('ERROR', `❌ [MEV加密失败] 自动回退明文交易发送: ${err.message}`);
    // 修正内存中的 Nonce 追踪：因为明文只用 1 个 nonce，下一个 nonce 应该为 outerNonce + 1
    nextNonceByAddress.set(address, outerNonce + 1);
    // 使用 outerNonce 发送明文交易以维持 nonce 序列的连续性
    return sendTx(innerTx, pair, txTimeoutMs, tip, outerNonce, meta);
  }

  // C. 调用底层发送器发送外层加密包装交易
  const sendRes = await sendTx(outerTx, pair, txTimeoutMs, tip, outerNonce, meta);
  if (!sendRes.success) {
    return sendRes;
  }

  // D. 外层发送成功，匹配解耦的区块事件以确定内层执行成败
  let targetId = null;
  if (sendRes.events && sendRes.events.length > 0) {
    const submittedEvent = sendRes.events.find(({ event }) => event.section === 'mevShield' && event.method === 'EncryptedSubmitted');
    if (submittedEvent) {
      const idVal = submittedEvent.event.data.id || submittedEvent.event.data[0];
      if (idVal) targetId = idVal.toString();
    }
  }

  if (!targetId) {
    return { success: false, error: 'MevShield execution verification failed: EncryptedSubmitted event not captured' };
  }

  const checkRes = await getMevShieldSuccess(sendRes.blockHash, targetId);
  if (checkRes.success) {
    return {
      success: true,
      hash: sendRes.hash,
      outerHash: sendRes.hash,
      innerHash: innerSignedHash,
      isEncrypted: true,
      blockHash: sendRes.blockHash,
      blockNumber: sendRes.blockNumber,
      txIndex: sendRes.txIndex
    };
  } else {
    return { success: false, error: checkRes.error };
  }
}

// 路由器层：策略路由分配器
async function sendStrategicTx(tx, pair, txTimeoutMs = 15000, tip = 0, meta = null) {
  const settings = database.getSettings();
  let shouldEncrypt = false;
  
  if (meta && meta.label) {
    const label = meta.label;
    if (label.startsWith('新子网打新') || label.startsWith('打新')) {
      shouldEncrypt = !!settings.dashingMevShieldEnabled;
    } else if (label.startsWith('改名抢跑')) {
      shouldEncrypt = !!settings.renameMevShieldEnabled;
    } else if (label.startsWith('冷键交换抢跑')) {
      shouldEncrypt = !!settings.swapMevShieldEnabled;
    }
  }

  if (shouldEncrypt) {
    return sendMevShieldTx(tx, pair, txTimeoutMs, tip, meta);
  } else {
    const res = await sendTx(tx, pair, txTimeoutMs, tip, null, meta);
    if (res.success) {
      return {
        success: true,
        hash: res.hash,
        outerHash: res.hash,
        innerHash: null,
        isEncrypted: false,
        blockHash: res.blockHash,
        blockNumber: res.blockNumber,
        txIndex: res.txIndex
      };
    }
    return res;
  }
}
```

#### 2.1.3 升级 `refreshAllWallets` 的钱包余额明细打印
```javascript
// 升级 refreshAllWallets 输出树状钱包统计
    log('INFO', `[钱包状态同步] 批量同步完成：`);
    for (let i = 0; i < wallets.length; i++) {
      const address = addresses[i];
      const account = accounts[i];
      const nextNonce = Number(nonces[i].toString());
      
      const freePlanck = BigInt(account.data.free.toString());
      const freeTao = Number(freePlanck) / 1e9;
      
      balanceByAddress.set(address, { freeTao, updatedAt: new Date(Date.now() + 8 * 3600000).toISOString() });
      nextNonceByAddress.set(address, nextNonce);
      
      log('INFO', `  ├─ 钱包【${wallets[i].name}】: 余额 ${freeTao.toFixed(2)} TAO | 链上 Nonce: ${nextNonce}`);
    }
```

#### 2.1.4 升级 `poll` 轮询响应时间检测
```javascript
// poll() 中检测 RPC 连接耗时
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
```

---

## 3. 验证与部署计划 (Verification & Deploy Plan)

### 3.1 语法检测与自研运行
* 应用以上改动后，在终端运行语法检查：
  `node -c bot.js`
* 保证没有任何 JavaScript 解释语法错误。

### 3.2 Git 上传
* 运行 `git diff` 进行最后一轮核实。
* 经由您的确认，执行提交和推送：
  `git add bot.js`
  `git commit -m "feat: upgrade console logs and implement custom Telegram HTML templates"`
  `git push origin main`
