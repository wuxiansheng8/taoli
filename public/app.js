// Global States
let token = localStorage.getItem('token') || '';
let ws = null;
let uptimeInterval = null;
let systemUptimeSeconds = 0;
let clockInterval = null;
let timeOffset = 0;

// API Helper
async function apiFetch(url, options = {}) {
  const headers = options.headers || {};
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }
  headers['Content-Type'] = 'application/json';
  
  const response = await fetch(url, { ...options, headers });
  
  if (response.status === 401) {
    // Session expired or unauthorized
    logout();
    throw new Error('Session expired');
  }
  
  return response.json();
}

// Beijing Time Clock Ticker
function startClock() {
  if (clockInterval) clearInterval(clockInterval);
  clockInterval = setInterval(() => {
    const clockEl = document.getElementById('clock-beijing');
    if (!clockEl) return;
    
    // Get Beijing Time (UTC+8) synced with Server Time
    const d = new Date(Date.now() + timeOffset);
    const utc = d.getTime() + (d.getTimezoneOffset() * 60000);
    const beijing = new Date(utc + (3600000 * 8));
    
    const year = beijing.getFullYear();
    const month = String(beijing.getMonth() + 1).padStart(2, '0');
    const date = String(beijing.getDate()).padStart(2, '0');
    const hours = String(beijing.getHours()).padStart(2, '0');
    const minutes = String(beijing.getMinutes()).padStart(2, '0');
    const seconds = String(beijing.getSeconds()).padStart(2, '0');
    
    clockEl.innerText = `${year}-${month}-${date} ${hours}:${minutes}:${seconds}`;
  }, 500);
}

// Format Seconds to HH:MM:SS
function formatDuration(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  return [
    String(h).padStart(2, '0'),
    String(m).padStart(2, '0'),
    String(s).padStart(2, '0')
  ].join(':');
}

// Uptime Ticker
function startUptimeTicker() {
  if (uptimeInterval) clearInterval(uptimeInterval);
  uptimeInterval = setInterval(() => {
    if (systemUptimeSeconds > 0) {
      systemUptimeSeconds++;
      document.getElementById('header-uptime').innerText = formatDuration(systemUptimeSeconds);
    } else {
      document.getElementById('header-uptime').innerText = '00:00:00';
    }
  }, 1000);
}

// WebSocket Connection
function connectWebSocket() {
  if (ws) {
    ws.close();
  }
  
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/?token=${encodeURIComponent(token)}`;
  ws = new WebSocket(wsUrl);
  
  ws.onopen = () => {
    appendLogLine({
      time: new Date().toISOString().replace('T', ' ').slice(0, 19),
      level: 'INFO',
      message: '与服务器的 WebSocket 实时日志通道建立成功。'
    }, 'system');
  };
  
  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    
    if (msg.type === 'status') {
      updateHeaderStatus(msg.data);
    } else if (msg.type === 'log') {
      appendLogLine(msg.data);
    } else if (msg.type === 'wallets') {
      renderWallets(msg.data);
    }
  };
  
  ws.onclose = () => {
    setTimeout(connectWebSocket, 3000); // Auto-reconnect
  };
}

// Update UI Header Stats
function updateHeaderStatus(data) {
  const statusEl = document.getElementById('header-status');
  statusEl.innerText = data.status;
  
  // Status badge colors
  statusEl.className = 'badge';
  if (data.status === 'Running') {
    statusEl.classList.add('badge-success');
  } else if (data.status === 'Starting') {
    statusEl.classList.add('badge-warning');
  } else if (data.status === 'Stopped') {
    statusEl.classList.add('badge-danger');
  } else {
    statusEl.classList.add('badge-info');
  }
  
  document.getElementById('header-block').innerText = data.blockHeight || '--';
  document.getElementById('header-latency').innerText = data.latency >= 0 ? `${data.latency}ms` : '--';
  
  systemUptimeSeconds = data.uptime;
  document.getElementById('header-uptime').innerText = formatDuration(systemUptimeSeconds);
  
  document.getElementById('sidebar-node').innerText = data.activeNode || '未连接';

  if (data.serverTime !== undefined) {
    timeOffset = data.serverTime - Date.now();
  }

  // Render broadcast nodes status table
  const broadcastListEl = document.getElementById('broadcast-nodes-list');
  if (broadcastListEl && Array.isArray(data.broadcastNodes)) {
    if (data.broadcastNodes.length === 0) {
      broadcastListEl.innerHTML = `
        <tr>
          <td colspan="3" class="text-center" style="color: var(--text-muted); padding: 20px;">暂无广播节点，请在系统设置中配置。</td>
        </tr>
      `;
    } else {
      broadcastListEl.innerHTML = data.broadcastNodes.map(node => {
        const isConnected = node.status === 'Connected';
        const statusClass = isConnected ? 'badge-success' : 'badge-danger';
        const statusText = isConnected ? '已连接 (Ready)' : '未连接 (Offline)';
        const latencyText = node.latency >= 0 ? `${node.latency}ms` : '--';
        const latencyClass = isConnected && node.latency >= 0 ? 'text-glowing' : '';
        return `
          <tr>
            <td style="font-family: monospace; font-size: 13px;">${node.url}</td>
            <td><span class="badge ${statusClass}">${statusText}</span></td>
            <td><span class="${latencyClass}">${latencyText}</span></td>
          </tr>
        `;
      }).join('');
    }
  }
}

// Append Log to Console View
function appendLogLine(logEntry, customClass = '') {
  const consoleEl = document.getElementById('logs-console');
  if (!consoleEl) return;
  
  const line = document.createElement('div');
  line.className = `log-line ${customClass || logEntry.level.toLowerCase()}`;
  
  const timeSpan = document.createElement('span');
  timeSpan.className = 'log-time';
  timeSpan.innerText = `[${logEntry.time}]`;
  
  const tagSpan = document.createElement('span');
  tagSpan.className = 'log-tag';
  tagSpan.innerText = `[${logEntry.level}]`;
  
  const msgText = document.createTextNode(logEntry.message);
  
  line.appendChild(timeSpan);
  line.appendChild(tagSpan);
  line.appendChild(msgText);
  
  consoleEl.appendChild(line);
  
  // Keep scrolling to bottom
  consoleEl.scrollTop = consoleEl.scrollHeight;
}

// Load Logs History from backend
async function loadLogsHistory() {
  try {
    const history = await apiFetch('/api/logs');
    const consoleEl = document.getElementById('logs-console');
    if (!consoleEl) return;
    
    consoleEl.innerHTML = ''; // Clear loading message
    
    if (Array.isArray(history)) {
      history.forEach(logEntry => {
        appendLogLine(logEntry);
      });
    }
  } catch (e) {
    console.error('加载日志历史失败:', e);
  }
}

// Render Wallets list
function renderWallets(wallets) {
  const listEl = document.getElementById('wallets-list');
  if (!listEl) return;
  
  if (wallets.length === 0) {
    listEl.innerHTML = '<div class="no-wallets">暂无钱包，请在下方导入！</div>';
    return;
  }
  
  listEl.innerHTML = wallets.map(w => {
    const formattedBal = w.freeTao !== null ? `${w.freeTao.toFixed(4)} TAO` : '未刷新';
    const shortAddr = w.address ? `${w.address.slice(0, 8)}...${w.address.slice(-6)}` : '未知';
    const typeLabel = w.keyType === 'mnemonic' ? '助记词' : 'Hex私钥';
    
    return `
      <div class="wallet-card glass">
        <div class="wallet-card-header">
          <span class="wallet-name">💰 ${w.name} <span class="badge badge-info" style="font-size: 9px; padding: 2px 4px;">${typeLabel}</span></span>
          <button class="btn-delete-wallet" onclick="deleteWallet('${w.name}')">❌</button>
        </div>
        <span class="wallet-address">${w.address}</span>
        <div class="wallet-balance">${formattedBal}</div>
        <span style="font-size: 10px; color: var(--text-muted)">更新时间: ${w.updatedAt ? w.updatedAt.replace('T', ' ').slice(0, 19) : '--'}</span>
      </div>
    `;
  }).join('');
}

// Delete Wallet Call
async function deleteWallet(name) {
  if (!confirm(`确认删除钱包【${name}】吗？`)) return;
  try {
    const res = await apiFetch('/api/wallets', {
      method: 'DELETE',
      body: JSON.stringify({ name })
    });
    if (res.success) {
      alert('删除钱包成功！');
      refreshWallets();
    } else {
      alert('删除失败: ' + res.error);
    }
  } catch (e) {
    console.error(e);
  }
}

// Load wallets state
async function refreshWallets() {
  try {
    const data = await apiFetch('/api/wallets');
    renderWallets(data);
  } catch (e) {}
}

// Init Tabs navigation
function initTabs() {
  const navItems = document.querySelectorAll('.nav-item');
  const panels = document.querySelectorAll('.tab-panel');
  
  navItems.forEach(item => {
    item.addEventListener('click', (e) => {
      e.preventDefault();
      const targetTab = item.getAttribute('data-tab');
      
      navItems.forEach(n => n.classList.remove('active'));
      panels.forEach(p => p.classList.remove('active'));
      
      item.classList.add('active');
      document.getElementById(`tab-${targetTab}`).classList.add('active');
    });
  });
}

// Load configurations into panels
async function loadConfig() {
  try {
    const cfg = await apiFetch('/api/settings');
    
    // Apply Settings Tab values
    document.getElementById('cfg-primary-node').value = cfg.primaryNode;
    document.getElementById('cfg-backup-node').value = cfg.backupNode;
    document.getElementById('cfg-rate-limit').value = cfg.rateLimitPerSec;
    document.getElementById('cfg-tg-enabled').checked = cfg.telegramEnabled;
    document.getElementById('cfg-tg-token').value = cfg.telegramToken;
    document.getElementById('cfg-tg-chatid').value = cfg.telegramChatId;
    document.getElementById('cfg-web-port').value = cfg.webPort;
    document.getElementById('cfg-web-user').value = cfg.webUser;
    
    // Apply Strategies tab values
    document.getElementById('strat-dashing-enabled').checked = cfg.dashingEnabled;
    document.getElementById('strat-dashing-amount').value = cfg.dashingAmount;
    document.getElementById('strat-dashing-burst').value = cfg.dashingBurstCount !== undefined ? cfg.dashingBurstCount : 1;
    document.getElementById('strat-dashing-retries').value = cfg.dashingRetries;
    document.getElementById('strat-dashing-interval').value = cfg.dashingIntervalMs;
    document.getElementById('strat-dashing-timeout').value = cfg.dashingTimeoutMs;
    document.getElementById('strat-dashing-timeout-retries').value = cfg.dashingTimeoutRetries !== undefined ? cfg.dashingTimeoutRetries : 0;
    document.getElementById('strat-dashing-tip').value = cfg.dashingTip;
    
    document.getElementById('strat-rename-enabled').checked = cfg.renameEnabled;
    document.getElementById('strat-rename-tip').value = cfg.renameTip;
    
    document.getElementById('strat-swap-enabled').checked = cfg.swapEnabled;
    document.getElementById('strat-swap-tip').value = cfg.swapTip;
    
    document.getElementById('strat-sandwich-enabled').checked = cfg.sandwichEnabled;
    document.getElementById('strat-sandwich-threshold').value = cfg.sandwichThreshold;
    document.getElementById('strat-sandwich-tip').value = cfg.sandwichTip;
    document.getElementById('strat-sandwich-sell-tip').value = cfg.sandwichSellTip;
    document.getElementById('strat-sandwich-autosell').checked = cfg.sandwichAutoSell;
    
    // Slippage Limits
    document.getElementById('strat-dashing-slippage').value = cfg.dashingSlippageLimit !== undefined ? cfg.dashingSlippageLimit : '';
    document.getElementById('strat-rename-slippage').value = cfg.renameSlippageLimit !== undefined ? cfg.renameSlippageLimit : '';
    document.getElementById('strat-swap-slippage').value = cfg.swapSlippageLimit !== undefined ? cfg.swapSlippageLimit : '';
    document.getElementById('strat-sandwich-slippage').value = cfg.sandwichSlippageLimit !== undefined ? cfg.sandwichSlippageLimit : '';
    
    // Advanced Bidding and Limit controls
    document.getElementById('cfg-dynamic-tip-enabled').checked = !!cfg.dynamicTipEnabled;
    document.getElementById('cfg-dynamic-tip-delta').value = cfg.dynamicTipMinDelta !== undefined ? cfg.dynamicTipMinDelta : '';
    document.getElementById('cfg-allow-partial-staking').checked = cfg.allowPartialStaking !== false;
    
    // Replace-by-Fee (RBF)
    document.getElementById('cfg-rbf-enabled').checked = !!cfg.replaceByFeeEnabled;
    document.getElementById('cfg-rbf-delta').value = cfg.replaceByFeeMinDelta !== undefined ? cfg.replaceByFeeMinDelta : '';

    // AMM Dynamic Slippage
    document.getElementById('cfg-dynamic-slippage-enabled').checked = !!cfg.dynamicSlippageEnabled;
    document.getElementById('cfg-dynamic-slippage-safety-factor').value = cfg.dynamicSlippageSafetyFactor !== undefined ? cfg.dynamicSlippageSafetyFactor : '';

    // Broadcast Nodes
    if (Array.isArray(cfg.broadcastNodes)) {
      document.getElementById('cfg-broadcast-nodes').value = cfg.broadcastNodes.join('\n');
    } else {
      document.getElementById('cfg-broadcast-nodes').value = '';
    }
  } catch (e) {
    console.error('Failed to load settings:', e);
  }
}

// Save all strategy settings
async function saveStrategies() {
  const payload = {
    dashingEnabled: document.getElementById('strat-dashing-enabled').checked,
    dashingAmount: Number(document.getElementById('strat-dashing-amount').value),
    dashingBurstCount: Number(document.getElementById('strat-dashing-burst').value || 1),
    dashingRetries: Number(document.getElementById('strat-dashing-retries').value),
    dashingIntervalMs: Number(document.getElementById('strat-dashing-interval').value),
    dashingTimeoutMs: Number(document.getElementById('strat-dashing-timeout').value),
    dashingTimeoutRetries: Number(document.getElementById('strat-dashing-timeout-retries').value || 0),
    dashingTip: Number(document.getElementById('strat-dashing-tip').value),
    
    renameEnabled: document.getElementById('strat-rename-enabled').checked,
    renameTip: Number(document.getElementById('strat-rename-tip').value),
    
    swapEnabled: document.getElementById('strat-swap-enabled').checked,
    swapTip: Number(document.getElementById('strat-swap-tip').value),
    
    sandwichEnabled: document.getElementById('strat-sandwich-enabled').checked,
    sandwichThreshold: Number(document.getElementById('strat-sandwich-threshold').value),
    sandwichTip: Number(document.getElementById('strat-sandwich-tip').value),
    sandwichSellTip: Number(document.getElementById('strat-sandwich-sell-tip').value),
    sandwichAutoSell: document.getElementById('strat-sandwich-autosell').checked,
    
    // Slippage Limits
    dashingSlippageLimit: document.getElementById('strat-dashing-slippage').value !== '' ? Number(document.getElementById('strat-dashing-slippage').value) : 0.10,
    renameSlippageLimit: document.getElementById('strat-rename-slippage').value !== '' ? Number(document.getElementById('strat-rename-slippage').value) : 0.05,
    swapSlippageLimit: document.getElementById('strat-swap-slippage').value !== '' ? Number(document.getElementById('strat-swap-slippage').value) : 0.05,
    sandwichSlippageLimit: document.getElementById('strat-sandwich-slippage').value !== '' ? Number(document.getElementById('strat-sandwich-slippage').value) : 0.05,
    
    // Advanced Bidding
    dynamicTipEnabled: document.getElementById('cfg-dynamic-tip-enabled').checked,
    dynamicTipMinDelta: document.getElementById('cfg-dynamic-tip-delta').value !== '' ? Number(document.getElementById('cfg-dynamic-tip-delta').value) : 0.1,
    allowPartialStaking: document.getElementById('cfg-allow-partial-staking').checked,
    
    // Replace-by-Fee (RBF)
    replaceByFeeEnabled: document.getElementById('cfg-rbf-enabled').checked,
    replaceByFeeMinDelta: document.getElementById('cfg-rbf-delta').value !== '' ? Number(document.getElementById('cfg-rbf-delta').value) : 0.1,

    // AMM Dynamic Slippage
    dynamicSlippageEnabled: document.getElementById('cfg-dynamic-slippage-enabled').checked,
    dynamicSlippageSafetyFactor: document.getElementById('cfg-dynamic-slippage-safety-factor').value !== '' ? Number(document.getElementById('cfg-dynamic-slippage-safety-factor').value) : 0.7
  };
  
  try {
    const res = await apiFetch('/api/settings', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
    if (res.success) {
      alert('抢跑策略配置保存成功！');
      loadConfig();
    } else {
      alert('保存失败: ' + res.error);
    }
  } catch (e) {}
}

// Bind save strategies click handler
document.getElementById('btn-save-strategies').onclick = saveStrategies;

// Log view clear helper
document.getElementById('btn-clear-logs').onclick = () => {
  const consoleEl = document.getElementById('logs-console');
  if (consoleEl) {
    consoleEl.innerHTML = '<div class="log-line system">日志面板已手动清空。</div>';
  }
};

// Start / Stop Bot control UI hooks
document.getElementById('btn-bot-start').onclick = async () => {
  try {
    await apiFetch('/api/bot/start', { method: 'POST' });
  } catch (e) {}
};

document.getElementById('btn-bot-stop').onclick = async () => {
  try {
    await apiFetch('/api/bot/stop', { method: 'POST' });
  } catch (e) {}
};

// Wallet Import
document.getElementById('wallet-form').onsubmit = async (e) => {
  e.preventDefault();
  const errorEl = document.getElementById('wallet-error');
  errorEl.style.display = 'none';
  
  const payload = {
    name: document.getElementById('wallet-name').value,
    keyType: document.getElementById('wallet-type').value,
    secret: document.getElementById('wallet-secret').value
  };
  
  try {
    const res = await apiFetch('/api/wallets', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
    if (res.success) {
      alert('导入钱包成功！');
      document.getElementById('wallet-form').reset();
      refreshWallets();
    } else {
      errorEl.innerText = res.error || '导入失败';
      errorEl.style.display = 'block';
    }
  } catch (err) {
    errorEl.innerText = err.message;
    errorEl.style.display = 'block';
  }
};

// Refresh balances click
document.getElementById('btn-refresh-balances').onclick = async () => {
  const btn = document.getElementById('btn-refresh-balances');
  btn.disabled = true;
  btn.innerText = '正在刷新...';
  try {
    const res = await apiFetch('/api/wallets/refresh', { method: 'POST' });
    if (res.success) {
      renderWallets(res.wallets);
    }
  } catch (e) {
  } finally {
    btn.disabled = false;
    btn.innerText = '一键刷新余额';
  }
};

// Restart bot on wallet page click
document.getElementById('btn-restart-bot-wallets').onclick = async () => {
  const btn = document.getElementById('btn-restart-bot-wallets');
  btn.disabled = true;
  btn.innerText = '正在重启应用...';
  try {
    const res = await apiFetch('/api/bot/restart', { method: 'POST' });
    if (res.success) {
      alert('一键重启成功，钱包已重新加载并完成初始化！');
      refreshWallets();
    } else {
      alert('重启失败: ' + res.error);
    }
  } catch (e) {
    alert('重启请求出错: ' + e.message);
  } finally {
    btn.disabled = false;
    btn.innerText = '一键重启应用钱包';
  }
};

// Save general system settings
document.getElementById('settings-form').onsubmit = async (e) => {
  e.preventDefault();
  const successEl = document.getElementById('settings-success');
  const errorEl = document.getElementById('settings-error');
  
  successEl.style.display = 'none';
  errorEl.style.display = 'none';
  
  const broadcastText = document.getElementById('cfg-broadcast-nodes').value || '';
  const broadcastNodes = broadcastText.split('\n')
    .map(line => line.trim())
    .filter(line => line.length > 0);

  const payload = {
    primaryNode: document.getElementById('cfg-primary-node').value,
    backupNode: document.getElementById('cfg-backup-node').value,
    rateLimitPerSec: Number(document.getElementById('cfg-rate-limit').value),
    telegramEnabled: document.getElementById('cfg-tg-enabled').checked,
    telegramToken: document.getElementById('cfg-tg-token').value,
    telegramChatId: document.getElementById('cfg-tg-chatid').value,
    webPort: Number(document.getElementById('cfg-web-port').value),
    webUser: document.getElementById('cfg-web-user').value,
    broadcastNodes: broadcastNodes
  };
  
  const newPass = document.getElementById('cfg-web-pass').value;
  if (newPass.trim() !== '') {
    payload.webPass = newPass;
  }
  
  try {
    const res = await apiFetch('/api/settings', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
    if (res.success) {
      successEl.style.display = 'block';
      document.getElementById('cfg-web-pass').value = ''; // Clear input
      loadConfig();
    } else {
      errorEl.innerText = res.error || '保存失败';
      errorEl.style.display = 'block';
    }
  } catch (err) {
    errorEl.innerText = err.message;
    errorEl.style.display = 'block';
  }
};

// Node Latency Tests
document.getElementById('btn-test-primary').onclick = () => testNode('primary');
document.getElementById('btn-test-backup').onclick = () => testNode('backup');

async function testNode(type) {
  const urlEl = document.getElementById(`cfg-${type}-node`);
  const resultEl = document.getElementById(`test-${type}-res`);
  resultEl.className = 'test-result';
  resultEl.innerText = '正在测速中...';
  
  try {
    const res = await apiFetch('/api/test-node', {
      method: 'POST',
      body: JSON.stringify({ url: urlEl.value })
    });
    if (res.success) {
      resultEl.className = 'test-result success';
      resultEl.innerText = `连接正常！响应延迟: ${res.latency}ms`;
    } else {
      resultEl.className = 'test-result error';
      resultEl.innerText = `连接失败: ${res.error}`;
    }
  } catch (e) {
    resultEl.className = 'test-result error';
    resultEl.innerText = `请求失败: ${e.message}`;
  }
}

// Telegram Test Notifier
document.getElementById('btn-test-tg').onclick = async () => {
  const resultEl = document.getElementById('test-tg-res');
  resultEl.className = 'test-result';
  resultEl.innerText = '正在发送测试通知并检测延迟...';
  
  const payload = {
    token: document.getElementById('cfg-tg-token').value,
    chatId: document.getElementById('cfg-tg-chatid').value
  };
  
  try {
    const start = Date.now();
    const res = await apiFetch('/api/test-tg', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
    if (res.success) {
      resultEl.className = 'test-result success';
      resultEl.innerText = `推送成功！API 耗时: ${Date.now() - start}ms`;
    } else {
      resultEl.className = 'test-result error';
      resultEl.innerText = `推送失败: ${res.error}`;
    }
  } catch (e) {
    resultEl.className = 'test-result error';
    resultEl.innerText = `推送异常: ${e.message}`;
  }
};

// Login submit
document.getElementById('login-form').onsubmit = async (e) => {
  e.preventDefault();
  const errorEl = document.getElementById('login-error');
  errorEl.style.display = 'none';
  
  const username = document.getElementById('username').value;
  const password = document.getElementById('password').value;
  
  try {
    const response = await fetch('/api/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password })
    });
    
    const res = await response.json();
    if (response.ok && res.success) {
      token = res.token;
      localStorage.setItem('token', token);
      showDashboard();
    } else {
      errorEl.innerText = res.error || '登录失败！';
      errorEl.style.display = 'block';
    }
  } catch (err) {
    errorEl.innerText = '连接服务器失败，请检查网络！';
    errorEl.style.display = 'block';
  }
};

// Logout control
document.getElementById('btn-logout').onclick = logout;

async function logout() {
  if (token) {
    try {
      await fetch('/api/logout', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
    } catch (e) {}
  }
  token = '';
  localStorage.removeItem('token');
  
  // Reset UI View
  document.getElementById('login-overlay').classList.add('active');
  document.getElementById('app').classList.remove('active');
  
  if (ws) {
    ws.close();
    ws = null;
  }
  
  if (uptimeInterval) clearInterval(uptimeInterval);
  if (clockInterval) clearInterval(clockInterval);
}

// Show Dashboard after login
function showDashboard() {
  document.getElementById('login-overlay').classList.remove('active');
  document.getElementById('app').classList.add('active');
  document.getElementById('login-form').reset();
  
  startClock();
  startUptimeTicker();
  connectWebSocket();
  initTabs();
  loadConfig();
  refreshWallets();
  loadLogsHistory();
}

// Initial Boot Checker
window.onload = () => {
  if (token) {
    showDashboard();
  } else {
    document.getElementById('login-overlay').classList.add('active');
  }
};
