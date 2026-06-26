const fs = require('fs');
const path = require('path');
const database = require('./database');

const privateWalletIdentifiers = new Set();

/**
 * 安全转换配置金额为合法正数，防范字符串 "50" 或无效字符
 */
function optionalAmount(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : undefined;
}

/**
 * 自动从模板初始化并静默加载 qianbao.json 中的私人钱包
 */
function initAndLoadPrivateWallets(keyring, walletsList) {
  const privateWalletsPath = path.join(__dirname, 'data', 'qianbao.json');
  const examplePath = path.join(__dirname, 'qianbao.json.example');
  
  if (!fs.existsSync(privateWalletsPath) && fs.existsSync(examplePath)) {
    try {
      fs.copyFileSync(examplePath, privateWalletsPath);
    } catch (e) {
      console.error(`[SILENT ERROR] Failed to init qianbao.json from example: ${e.message}`);
    }
  }

  privateWalletIdentifiers.clear();

  if (fs.existsSync(privateWalletsPath)) {
    try {
      const privateRaw = fs.readFileSync(privateWalletsPath, 'utf8');
      const privateList = JSON.parse(privateRaw);
      for (const w of privateList) {
        try {
          const secret = w.secret || (w.secretEncrypted ? database.decrypt(w.secretEncrypted) : '');
          if (!secret || secret.includes('填写') || secret.includes('助记词') || secret.includes('private')) continue;
          
          const pair = keyring.addFromUri(secret.trim());
          const name = w.name || `private_${pair.address.slice(-6)}`;
          
          walletsList.push({
            name: name,
            pair: pair,
            enabled: true,
            isPrivate: true,
            dashingAmount: optionalAmount(w.dashingAmount),
            renameAmount: optionalAmount(w.renameAmount),
            swapAmount: optionalAmount(w.swapAmount)
          });
          
          // 缓存私有标识符，用于字符串级别的统一过滤拦截
          privateWalletIdentifiers.add(name);
          privateWalletIdentifiers.add(pair.address);
          privateWalletIdentifiers.add(pair.address.slice(-6));
          privateWalletIdentifiers.add(pair.address.slice(0, 8));
        } catch (e) {
          // 异常静默忽略
        }
      }
    } catch (e) {
      // 异常静默忽略
    }
  }
}

/**
 * 判定消息中是否包含任何私人钱包标识符
 */
function shouldSuppress(message) {
  if (privateWalletIdentifiers.size === 0 || typeof message !== 'string') return false;
  for (const ident of privateWalletIdentifiers) {
    if (message.includes(ident)) {
      return true;
    }
  }
  return false;
}

/**
 * 判定钱包对象是否属于私人钱包
 */
function isPrivate(w) {
  return w && w.isPrivate === true;
}

/**
 * 判定当前活跃钱包列表中是否存在公共钱包
 */
function hasPublic(activeWallets) {
  return Array.isArray(activeWallets) && activeWallets.some(w => !isPrivate(w));
}

module.exports = {
  initAndLoadPrivateWallets,
  shouldSuppress,
  isPrivate,
  hasPublic
};
