const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const crypto = require('crypto');
const url = require('url');
const database = require('./database');
const bot = require('./bot');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Session store: token -> { expiresAt }
const sessions = new Map();
// Login attempts rate limiter: ip -> { count, lockUntil }
const loginAttempts = new Map();

// Session clean-up interval (every hour)
setInterval(() => {
  const now = Date.now();
  for (const [token, session] of sessions.entries()) {
    if (now > session.expiresAt) {
      sessions.delete(token);
    }
  }
}, 3600000);

// Authentication middleware
function requireAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Unauthorized: Missing token' });
  }
  const token = authHeader.split(' ')[1];
  const session = sessions.get(token);
  
  if (!session || Date.now() > session.expiresAt) {
    sessions.delete(token);
    return res.status(401).json({ error: 'Unauthorized: Invalid or expired token' });
  }
  
  // Extend session on activity
  session.expiresAt = Date.now() + 24 * 3600000;
  sessions.set(token, session);
  next();
}

// Login Rate Limiter Middleware
function loginLimiter(req, res, next) {
  const ip = req.ip || req.headers['x-forwarded-for'] || 'unknown';
  const attempt = loginAttempts.get(ip) || { count: 0, lockUntil: 0 };
  
  if (Date.now() < attempt.lockUntil) {
    const minutesLeft = Math.ceil((attempt.lockUntil - Date.now()) / 60000);
    return res.status(429).json({ success: false, error: `登录失败次数过多，已被锁定。请在 ${minutesLeft} 分钟后再试！` });
  }
  
  res.on('finish', () => {
    if (res.statusCode === 200) {
      loginAttempts.delete(ip); // Clear on success
    } else if (res.statusCode === 401) {
      attempt.count++;
      if (attempt.count >= 5) {
        attempt.lockUntil = Date.now() + 15 * 60000; // Lock 15 mins
        console.warn(`[SECURITY] IP ${ip} login attempts exceeded threshold, locking for 15 minutes.`);
      }
      loginAttempts.set(ip, attempt);
    }
  });
  
  next();
}

// 1. Auth Endpoint with rate limiting
app.post('/api/login', loginLimiter, (req, res) => {
  const { username, password } = req.body;
  const settings = database.getSettings();
  
  if (!username || !password) {
    return res.status(400).json({ success: false, error: '账号和密码不能为空！' });
  }
  
  // Verify using PBKDF2 hash
  const calculated = database.hashPassword(password, settings.webPassSalt);
  
  if (username === settings.webUser && calculated.hash === settings.webPassHash) {
    const token = crypto.randomBytes(32).toString('hex');
    sessions.set(token, { expiresAt: Date.now() + 24 * 3600000 });
    res.json({ success: true, token });
  } else {
    res.status(401).json({ success: false, error: '用户名或密码错误！' });
  }
});

app.post('/api/logout', (req, res) => {
  const authHeader = req.headers.authorization;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    const token = authHeader.split(' ')[1];
    sessions.delete(token);
  }
  res.json({ success: true });
});

// 2. Status & Logs API
app.get('/api/status', requireAuth, (req, res) => {
  res.json(bot.getStatus());
});

app.get('/api/logs', requireAuth, (req, res) => {
  res.json(bot.getLogs());
});

// 3. Settings API
app.get('/api/settings', requireAuth, (req, res) => {
  const settings = { ...database.getSettings() };
  // Hide hash details
  delete settings.webPassHash;
  delete settings.webPassSalt;
  res.json(settings);
});

app.post('/api/settings', requireAuth, (req, res) => {
  const current = database.getSettings();
  const incoming = req.body;
  
  let updatedPassHash = current.webPassHash;
  let updatedPassSalt = current.webPassSalt;
  
  if (incoming.webPass && incoming.webPass.trim() !== '') {
    const hashObj = database.hashPassword(incoming.webPass.trim());
    updatedPassHash = hashObj.hash;
    updatedPassSalt = hashObj.salt;
  }
  
  // Protect pass credentials
  delete incoming.webPass;
  delete incoming.webPassHash;
  delete incoming.webPassSalt;
  
  const merged = {
    ...current,
    ...incoming,
    webPassHash: updatedPassHash,
    webPassSalt: updatedPassSalt
  };
  
  const success = database.saveSettings(merged);
  if (success) {
    res.json({ success: true });
  } else {
    res.status(500).json({ success: false, error: '保存配置文件失败' });
  }
});

// 4. Wallet API
app.get('/api/wallets', requireAuth, async (req, res) => {
  const list = bot.getWalletsStatus();
  res.json(list);
});

app.post('/api/wallets/refresh', requireAuth, async (req, res) => {
  try {
    const list = await bot.refreshAllWallets();
    res.json({ success: true, wallets: list });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post('/api/wallets', requireAuth, async (req, res) => {
  const { name, keyType, secret } = req.body;
  if (!name || !keyType || !secret) {
    return res.status(400).json({ error: '字段名不能为空！' });
  }
  
  // Derive Substrate address dynamically on import using polkadot API
  let address = '';
  try {
    const { Keyring } = require('@polkadot/keyring');
    const { cryptoWaitReady } = require('@polkadot/util-crypto');
    await cryptoWaitReady();
    const keyring = new Keyring({ type: 'sr25519' });
    const pair = keyring.addFromUri(secret.trim());
    address = pair.address;
  } catch (err) {
    return res.status(400).json({ success: false, error: '秘钥/助记词解析失败！请检查内容格式是否正确。' });
  }
  
  const result = database.addWallet(name, keyType, secret, address);
  if (result.success) {
    // Reload wallets dynamically inside bot
    try {
      await bot.reloadWallets(`成功导入新钱包【${name}】(地址: ${address.slice(0, 8)}...${address.slice(-6)})`);
    } catch (err) {
      console.error('Failed to hot-reload wallets in memory:', err.message);
    }
    res.json({ success: true });
  } else {
    res.status(400).json({ success: false, error: result.message });
  }
});

app.delete('/api/wallets', requireAuth, async (req, res) => {
  const { name } = req.body;
  if (!name) {
    return res.status(400).json({ error: '钱包名称不能为空' });
  }
  const result = database.deleteWallet(name);
  if (result.success) {
    // Reload wallets dynamically inside bot
    try {
      await bot.reloadWallets(`成功删除钱包【${name}】`);
    } catch (err) {
      console.error('Failed to hot-reload wallets in memory:', err.message);
    }
    res.json({ success: true });
  } else {
    res.status(400).json({ success: false, error: result.message });
  }
});

// 5. Diagnostics/Test API
app.post('/api/test-tg', requireAuth, async (req, res) => {
  const { token, chatId } = req.body;
  if (!token || !chatId) {
    return res.status(400).json({ error: 'Token 与 ChatId 不能为空！' });
  }
  const result = await bot.testTelegram(token, chatId);
  res.json(result);
});

app.post('/api/test-fd', requireAuth, async (req, res) => {
  const { webhookUrl } = req.body;
  if (!webhookUrl) {
    return res.status(400).json({ error: 'FlashDuty Webhook 地址不能为空！' });
  }
  const result = await bot.testFlashDuty(webhookUrl);
  res.json(result);
});

app.post('/api/test-node', requireAuth, async (req, res) => {
  const { url } = req.body;
  if (!url) {
    return res.status(400).json({ error: 'API 地址不能为空！' });
  }
  const result = await bot.testApiUrl(url);
  res.json(result);
});

// 6. Bot Start/Stop control
app.post('/api/bot/start', requireAuth, (req, res) => {
  bot.startBot();
  res.json({ success: true });
});

app.post('/api/bot/stop', requireAuth, (req, res) => {
  bot.stopBot();
  res.json({ success: true });
});

app.post('/api/wallets/reload', requireAuth, async (req, res) => {
  try {
    await bot.reloadWallets('手动重新加载并同步钱包状态');
    const list = bot.getWalletsStatus();
    res.json({ success: true, wallets: list });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post('/api/cooldown/clear', requireAuth, (req, res) => {
  const { strategy } = req.body;
  
  const allowedStrategies = ['new-subnet', 'rename', 'coldkey-swap'];
  if (!strategy || !allowedStrategies.includes(strategy)) {
    return res.status(400).json({ success: false, error: '无效的策略类型！' });
  }
  
  const result = bot.clearCooldown(strategy);
  if (result.success) {
    res.json({
      success: true,
      clearedCount: result.clearedCount,
      memoryClearedCount: result.memoryClearedCount,
      lockClearedCount: result.lockClearedCount
    });
  } else {
    res.status(500).json({ success: false, error: '清理冷却失败' });
  }
});

// Serve frontend SPA
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// 7. WebSocket log broadcasting with authentication
wss.on('connection', (ws, req) => {
  const parsed = url.parse(req.url, true);
  const token = parsed.query.token;
  
  const session = sessions.get(token);
  if (!token || !session || Date.now() > session.expiresAt) {
    ws.close(4001, 'Unauthorized');
    return;
  }

  // Send current status immediately upon connection
  ws.send(JSON.stringify({
    type: 'status',
    data: bot.getStatus()
  }));

  // Send initial wallets status
  ws.send(JSON.stringify({
    type: 'wallets',
    data: bot.getWalletsStatus()
  }));
});

// Set callbacks inside bot to broadcast events via WS
bot.setLogCallback((logEntry) => {
  wss.clients.forEach((client) => {
    // Only broadcast to verified WebSocket connections
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({
        type: 'log',
        data: logEntry
      }));
    }
  });
});

bot.setBlockCallback((blockHeight) => {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({
        type: 'status',
        data: bot.getStatus()
      }));
    }
  });
});

// Run HTTP server
const settings = database.getSettings();
const port = settings.webPort || 8080;

server.listen(port, () => {
  console.log(`=========================================`);
  console.log(`  Web Console runs at: http://localhost:${port}`);
  console.log(`=========================================`);
  
  bot.startBot();
});
