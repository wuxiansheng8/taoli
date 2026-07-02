const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const DATA_DIR = path.join(__dirname, 'data');
const SETTINGS_FILE = path.join(DATA_DIR, 'settings.json');
const WALLETS_FILE = path.join(DATA_DIR, 'wallets.json');
const KEY_FILE = path.join(DATA_DIR, '.key');
const COOLDOWNS_FILE = path.join(DATA_DIR, 'cooldowns.json');

const ENCRYPTION_ALGORITHM = 'aes-256-cbc';
const IV_LENGTH = 16;

// Create data directory if it doesn't exist
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Generate or read the dynamic machine-unique secret key
function getEncryptionKey() {
  if (!fs.existsSync(KEY_FILE)) {
    const key = crypto.randomBytes(32).toString('hex');
    fs.writeFileSync(KEY_FILE, key, 'utf8');
    try {
      fs.chmodSync(KEY_FILE, 0o600); // Strict permission on Linux
    } catch (e) {
      // Ignore on Windows
    }
  }
  const rawKey = fs.readFileSync(KEY_FILE, 'utf8').trim();
  return crypto.createHash('sha256').update(rawKey).digest();
}

// Password hashing helper (PBKDF2 with SHA-256)
function hashPassword(password, salt) {
  const finalSalt = salt || crypto.randomBytes(16).toString('hex');
  const hash = crypto.pbkdf2Sync(password, finalSalt, 1000, 64, 'sha256').toString('hex');
  return { hash, salt: finalSalt };
}

// Default settings (Pre-hashed admin123 password)
const defaultHashObj = hashPassword('admin123');
const DEFAULT_SETTINGS = {
  defaultHotkey: "",
  webPort: 8080,
  webUser: 'admin',
  webPassHash: defaultHashObj.hash,
  webPassSalt: defaultHashObj.salt,
  primaryNode: 'ws://127.0.0.1:9944',
  backupNode: 'wss://entrypoint-finney.opentensor.ai:443',
  mempoolPollIntervalMs: 100,
  nonceSyncIntervalSeconds: 60,
  
  telegramEnabled: false,

  telegramToken: '',
  telegramChatId: '',
  
  // FlashDuty settings
  flashDutyEnabled: false,
  flashDutyWebhookUrl: '',
  flashDutyCooldownMs: 300000, // 默认 5 分钟
  
  // Strategy: New Subnet Auto Register
  dashingEnabled: true,
  dashingAmount: 100, // TAO to register
  dashingRetries: 10,
  dashingIntervalMs: 1000,
  dashingTimeoutMs: 30000,
  dashingSlippageLimit: 0, // 留空为不限制相对滑点，直接为 0 (禁用)
  dashingBurstCount: 1, // number of concurrent staking transactions per wallet
  dashingDoubleStakingDelay: 0, // delay in seconds for second buy-in
  dashingDoubleSlippageLimit: 0, // 留空为不限制相对滑点，直接为 0 (禁用)
  dashingTimeoutRetries: 0,
  dashingMaxPrice: 0, // 主线最大价格限制 (TAO/Alpha, 0为无限制)
  dashingDoubleMaxPrice: 0, // 二次延迟最大价格限制 (TAO/Alpha, 0为无限制)
  
  // Strategy: Subnet Rename Frontrun
  renameEnabled: true,
  renameAmount: 100,
  renameSlippageLimit: 0.05, // 5% slippage
  renameRetries: 1,
  renameIntervalMs: 1000,
  renameTimeoutMs: 30000,
  renameBurstCount: 1,
  renameTimeoutRetries: 0,
  
  // Strategy: Coldkey Swap Frontrun
  swapEnabled: true,
  swapAmount: 100,
  swapSlippageLimit: 0.05, // 5% slippage
  swapRetries: 1,
  swapIntervalMs: 1000,
  swapTimeoutMs: 30000,
  swapBurstCount: 1,
  swapTimeoutRetries: 0,
  
  // Advanced priorities
  allowPartialStaking: true, // allow partial fill on limit orders
  broadcastNodes: [
    "wss://entrypoint-finney.opentensor.ai:443",
    "wss://archival.finney.opentensor.ai:443"
  ]
};

// Encryption helpers using dynamic key
function encrypt(text) {
  const key = getEncryptionKey();
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ENCRYPTION_ALGORITHM, key, iv);
  let encrypted = cipher.update(text, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  return iv.toString('hex') + ':' + encrypted;
}

function decrypt(text) {
  try {
    const key = getEncryptionKey();
    const textParts = text.split(':');
    const iv = Buffer.from(textParts.shift(), 'hex');
    const encryptedText = Buffer.from(textParts.join(':'), 'hex');
    const decipher = crypto.createDecipheriv(ENCRYPTION_ALGORITHM, key, iv);
    let decrypted = decipher.update(encryptedText, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
  } catch (e) {
    return 'DECRYPTION_ERROR';
  }
}

// Settings, Wallets and Cooldowns Memory Caches
let settingsCache = null;
let walletsCache = null;
let cooldownsCache = null;

// Initialize cache from disk (called once synchronously when module is required)
function initCache() {
  // 1. Load settings
  if (!fs.existsSync(SETTINGS_FILE)) {
    settingsCache = { ...DEFAULT_SETTINGS };
    saveSettings(DEFAULT_SETTINGS);
  } else {
    try {
      const rawData = fs.readFileSync(SETTINGS_FILE, 'utf8');
      const settings = JSON.parse(rawData);
      delete settings.dashingMevShieldEnabled;
      delete settings.renameMevShieldEnabled;
      delete settings.swapMevShieldEnabled;
      delete settings.sandwichEnabled;
      delete settings.sandwichAmount;
      delete settings.sandwichThreshold;
      delete settings.sandwichTip;
      delete settings.sandwichAutoSell;
      delete settings.sandwichSellTip;
      delete settings.sandwichSlippageLimit;
      delete settings.sandwichTimeoutMs;
      delete settings.dynamicSlippageEnabled;
      delete settings.dynamicSlippageSafetyFactor;
      delete settings.dashingTip;
      delete settings.renameTip;
      delete settings.swapTip;
      delete settings.rateLimitPerSec;
      settingsCache = { ...DEFAULT_SETTINGS, ...settings };
    } catch (e) {
      console.error('Error reading settings file, using defaults:', e);
      settingsCache = { ...DEFAULT_SETTINGS };
    }
  }

  // 2. Load wallets
  if (!fs.existsSync(WALLETS_FILE)) {
    walletsCache = [];
    saveWallets([]);
  } else {
    try {
      const rawData = fs.readFileSync(WALLETS_FILE, 'utf8');
      walletsCache = JSON.parse(rawData);
    } catch (e) {
      console.error('Error reading wallets file:', e);
      walletsCache = [];
    }
  }

  // 3. Load cooldowns
  if (!fs.existsSync(COOLDOWNS_FILE)) {
    cooldownsCache = {};
  } else {
    try {
      const rawData = fs.readFileSync(COOLDOWNS_FILE, 'utf8');
      cooldownsCache = JSON.parse(rawData);
    } catch (e) {
      console.error('Error reading cooldowns:', e);
      cooldownsCache = {};
    }
  }
}

// Automatically trigger initial caching
initCache();

// Settings management
function getSettings() {
  if (!settingsCache) initCache();
  return { ...settingsCache }; // Return shallow copy to prevent pollution
}

function saveSettings(settings) {
  try {
    settingsCache = { ...DEFAULT_SETTINGS, ...settings };
    // Asynchronous disk write
    fs.promises.writeFile(SETTINGS_FILE, JSON.stringify(settingsCache, null, 2), 'utf8')
      .catch(e => console.error('Error async writing settings:', e));
    return true;
  } catch (e) {
    console.error('Error writing settings memory:', e);
    return false;
  }
}

// Wallets management
function getWallets(shouldDecrypt = false) {
  if (!walletsCache) initCache();
  return walletsCache.map(wallet => {
    const w = { ...wallet };
    if (shouldDecrypt && w.secretEncrypted) {
      w.secret = decrypt(w.secretEncrypted);
    }
    // Never send encrypted key directly to UI with secret
    if (!shouldDecrypt) {
      delete w.secretEncrypted;
    }
    return w;
  });
}

function saveWallets(wallets) {
  try {
    walletsCache = wallets;
    // Asynchronous disk write
    fs.promises.writeFile(WALLETS_FILE, JSON.stringify(walletsCache, null, 2), 'utf8')
      .catch(e => console.error('Error async writing wallets:', e));
    return true;
  } catch (e) {
    console.error('Error writing wallets memory:', e);
    return false;
  }
}

function addWallet(name, keyType, secret, address) {
  const wallets = getWallets(true);
  
  if (wallets.some(w => w.name === name)) {
    return { success: false, message: 'Wallet name already exists' };
  }
  
  const encrypted = encrypt(secret.trim());
  const newWallet = {
    name: name.trim(),
    keyType: keyType,
    address: address.trim(),
    secretEncrypted: encrypted,
    createdAt: new Date().toISOString()
  };
  
  wallets.push(newWallet);
  
  const toSave = wallets.map(w => ({
    name: w.name,
    keyType: w.keyType,
    address: w.address,
    secretEncrypted: w.secretEncrypted || encrypt(w.secret),
    createdAt: w.createdAt
  }));
  
  saveWallets(toSave);
  return { success: true };
}

function deleteWallet(name) {
  const wallets = getWallets(true);
  const filtered = wallets.filter(w => w.name !== name);
  
  if (wallets.length === filtered.length) {
    return { success: false, message: 'Wallet not found' };
  }
  
  const toSave = filtered.map(w => ({
    name: w.name,
    keyType: w.keyType,
    address: w.address,
    secretEncrypted: w.secretEncrypted || encrypt(w.secret),
    createdAt: w.createdAt
  }));
  
  saveWallets(toSave);
  return { success: true };
}

function getCooldown(key) {
  if (!cooldownsCache) initCache();
  return cooldownsCache[key] || null;
}

function setCooldown(key, data) {
  try {
    if (!cooldownsCache) initCache();
    cooldownsCache[key] = {
      ...data,
      firstTriggeredAt: Date.now()
    };
    
    // Clean up expired cooldowns (older than 24 hours)
    const now = Date.now();
    const expiry = 24 * 60 * 60 * 1000;
    for (const k in cooldownsCache) {
      if (!cooldownsCache[k] || !cooldownsCache[k].firstTriggeredAt || now - cooldownsCache[k].firstTriggeredAt > expiry) {
        delete cooldownsCache[k];
      }
    }
    
    // Asynchronous disk write
    fs.promises.writeFile(COOLDOWNS_FILE, JSON.stringify(cooldownsCache, null, 2), 'utf8')
      .catch(e => console.error('Error async writing cooldowns:', e));
    return true;
  } catch (e) {
    console.error('Error writing cooldowns memory:', e);
    return false;
  }
}

function clearCooldownsByStrategy(strategyPrefix) {
  try {
    if (!cooldownsCache) initCache();
    let clearedCount = 0;
    
    for (const key in cooldownsCache) {
      if (
        (strategyPrefix === 'new-subnet' && (key.startsWith('new-subnet:') || key.startsWith('new-subnet-double:'))) ||
        (strategyPrefix === 'rename' && key.startsWith('rename:')) ||
        (strategyPrefix === 'coldkey-swap' && key.startsWith('coldkey-swap:'))
      ) {
        delete cooldownsCache[key];
        clearedCount++;
      }
    }
    
    // Asynchronous disk write
    fs.promises.writeFile(COOLDOWNS_FILE, JSON.stringify(cooldownsCache, null, 2), 'utf8')
      .catch(e => console.error('Error clearing cooldowns memory:', e));
    return clearedCount;
  } catch (e) {
    console.error('Error clearing cooldowns memory:', e);
    return 0;
  }
}

module.exports = {
  getSettings,
  saveSettings,
  getWallets,
  addWallet,
  deleteWallet,
  hashPassword,
  getCooldown,
  setCooldown,
  clearCooldownsByStrategy,
  decrypt
};
