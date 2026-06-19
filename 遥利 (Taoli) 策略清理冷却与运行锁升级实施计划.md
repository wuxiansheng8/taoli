# 添加策略 1/2/3 "清理冷却" (Clear Cooldown) 功能设计与实现方案

在 Bittensor Sniper 控制台的“抢跑策略”标签页中，为 **策略 1、策略 2 和策略 3** 的启用开关旁边分别添加一个 **“清理冷却”** 按钮。当用户点击该按钮并确认后，系统将通过 API 清理后端对该策略的持久化冷却缓存 (`cooldowns.json`) 和内存缓存 (`dashingSuccessByNetuid`)，以保证即使同一个子网再次触发相应策略的条件也能继续打新。

## 用户审查说明

> [!IMPORTANT]
> 遵照您的指示：**在您进行代码审查并决定下一步之前，不会对本地源码做任何修改，也不会提交到 GitHub**。本方案已作为 Artifact 呈现，请您审查所有文件的改动计划，并在确认无误后点击界面上的 **"Proceed"** 批准或在对话中告知我开始执行。

---

## 拟议的改动

本方案涉及 6 个文件的改动，从下至上分别为：数据库层 -> 机器人逻辑层 -> 后端路由 -> 前端样式 -> 前端结构 -> 前端控制。

### 1. 数据库访问层

#### [MODIFY] [database.js](file:///C:/Users/moshe/.gemini/antigravity/scratch/taoli/database.js)

添加 `clearCooldownsByStrategy` 函数，从 `cooldowns.json` 持久化文件中删除属于目标策略的冷却记录，并导出该函数。

```javascript
// 新增函数
function clearCooldownsByStrategy(strategyPrefix) {
  try {
    if (!fs.existsSync(COOLDOWNS_FILE)) return 0;
    const cooldowns = JSON.parse(fs.readFileSync(COOLDOWNS_FILE, 'utf8'));
    let clearedCount = 0;
    
    for (const key in cooldowns) {
      if (
        (strategyPrefix === 'new-subnet' && (key.startsWith('new-subnet:') || key.startsWith('new-subnet-double:'))) ||
        (strategyPrefix === 'rename' && key.startsWith('rename:')) ||
        (strategyPrefix === 'coldkey-swap' && key.startsWith('coldkey-swap:'))
      ) {
        delete cooldowns[key];
        clearedCount++;
      }
    }
    
    fs.writeFileSync(COOLDOWNS_FILE, JSON.stringify(cooldowns, null, 2), 'utf8');
    return clearedCount;
  } catch (e) {
    console.error('Error clearing cooldowns:', e);
    return 0;
  }
}

// 导出改动
module.exports = {
  // ... 其他已有导出
  getCooldown,
  setCooldown,
  clearCooldownsByStrategy // 新增导出
};
```

---

### 2. 机器人控制与内存状态层

#### [MODIFY] [bot.js](file:///C:/Users/moshe/.gemini/antigravity/scratch/taoli/bot.js)

在机器人逻辑层中，添加 `clearCooldown` 函数，清理数据库记录的同时清理内存中防重复执行的 `dashingSuccessByNetuid` 缓存，并在 `module.exports` 中导出该函数。

```javascript
// 新增函数
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

// 导出改动
module.exports = {
  // ... 其他已有导出
  setLogCallback: (cb) => { global.logCallback = cb; },
  setBlockCallback: (cb) => { global.blockCallback = cb; },
  clearCooldown // 新增导出
};
```

---

### 3. API Web 路由层

#### [MODIFY] [server.js](file:///C:/Users/moshe/.gemini/antigravity/scratch/taoli/server.js)

在 Web 控制台的路由列表中，添加一个经身份验证（`requireAuth`）的 POST 路由 `/api/cooldown/clear`。

```javascript
// 新增路由
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
```

---

### 4. 前端 HTML 结构

#### [MODIFY] [index.html](file:///C:/Users/moshe/.gemini/antigravity/scratch/taoli/public/index.html)

在 `index.html` 的策略 1、2、3 开关处，使用一个按钮与开关的组合容器 `<div class="strategy-action-group">` 将“清理冷却”按钮和 Toggle 开关包在一起。

**策略 1 改动 (约 L217 处):**
```diff
-              <label class="switch">
-                <input type="checkbox" id="strat-dashing-enabled">
-                <span class="slider"></span>
-              </label>
+              <div class="strategy-action-group">
+                <button type="button" class="btn btn-secondary btn-sm btn-clear-cooldown" data-strategy="new-subnet">清理冷却</button>
+                <label class="switch">
+                  <input type="checkbox" id="strat-dashing-enabled">
+                  <span class="slider"></span>
+                </label>
+              </div>
```

**策略 2 改动 (约 L284 处):**
```diff
-              <label class="switch">
-                <input type="checkbox" id="strat-rename-enabled">
-                <span class="slider"></span>
-              </label>
+              <div class="strategy-action-group">
+                <button type="button" class="btn btn-secondary btn-sm btn-clear-cooldown" data-strategy="rename">清理冷却</button>
+                <label class="switch">
+                  <input type="checkbox" id="strat-rename-enabled">
+                  <span class="slider"></span>
+                </label>
+              </div>
```

**策略 3 改动 (约 L332 处):**
```diff
-              <label class="switch">
-                <input type="checkbox" id="strat-swap-enabled">
-                <span class="slider"></span>
-              </label>
+              <div class="strategy-action-group">
+                <button type="button" class="btn btn-secondary btn-sm btn-clear-cooldown" data-strategy="coldkey-swap">清理冷却</button>
+                <label class="switch">
+                  <input type="checkbox" id="strat-swap-enabled">
+                  <span class="slider"></span>
+                </label>
+              </div>
```

---

### 5. 前端 CSS 样式

#### [MODIFY] [style.css](file:///C:/Users/moshe/.gemini/antigravity/scratch/taoli/public/style.css)

在样式文件底部追加以下 CSS 规则，美化放置于开关左侧的“清理冷却”按钮，使其与控制台原有的暗色科技风设计和 HSL/RGB 配色完全融合。

```css
/* 策略头部动作栏容器 */
.strategy-action-group {
  display: flex;
  align-items: center;
  gap: 12px;
}

/* 清理冷却按钮基础样式 */
.btn-clear-cooldown {
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid rgba(255, 255, 255, 0.1);
  color: var(--text-main);
  padding: 6px 12px;
  border-radius: 6px;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s cubic-bezier(0.165, 0.84, 0.44, 1);
}

/* 悬停态样式 */
.btn-clear-cooldown:hover {
  background: rgba(255, 255, 255, 0.12);
  border-color: rgba(255, 255, 255, 0.25);
  transform: translateY(-1px);
  box-shadow: 0 2px 8px rgba(255, 255, 255, 0.05);
}

/* 禁用态（清理中） */
.btn-clear-cooldown:disabled {
  opacity: 0.5;
  cursor: not-allowed;
  transform: none;
  box-shadow: none;
}
```

---

### 6. 前端控制与 API 调用

#### [MODIFY] [app.js](file:///C:/Users/moshe/.gemini/antigravity/scratch/taoli/public/app.js)

在 `app.js` 的末尾，注册“清理冷却”按钮的点击事件绑定。在用户点击时展示 `confirm` 对话框，确认后调用 `apiFetch` 并弹出结果。

```javascript
// 注册“清理冷却”按钮点击事件
document.querySelectorAll('.btn-clear-cooldown').forEach(btn => {
  btn.onclick = async () => {
    const strategy = btn.getAttribute('data-strategy');
    const strategyNamesMap = {
      'new-subnet': '策略 1 (新建立子网 Staking 抢购)',
      'rename': '策略 2 (子网改名抢跑)',
      'coldkey-swap': '策略 3 (冷键交换声明/执行抢跑)'
    };
    const strategyDisplayName = strategyNamesMap[strategy] || strategy;
    
    // 显示浏览器自带的确认框
    if (confirm(`确认要清理 ${strategyDisplayName} 的冷却与运行锁吗？\n\n该操作会删除 24 小时冷却、成功状态缓存，并强制释放当前策略的运行锁。\n\n请确认当前没有正在执行中的抢跑交易，否则可能导致重复买入。`)) {
      try {
        btn.disabled = true;
        const origText = btn.innerText;
        btn.innerText = '清理中...';
        
        // 调用封装好的 apiFetch，自动携带 Bearer Token 和 Content-Type
        const res = await apiFetch('/api/cooldown/clear', {
          method: 'POST',
          body: JSON.stringify({ strategy })
        });
        
        if (res && res.success) {
          alert(`清理成功！\n- 持久化冷却: ${res.clearedCount || 0} 条\n- 内存成功状态: ${res.memoryClearedCount || 0} 条\n- 运行锁: ${res.lockClearedCount || 0} 个`);
        } else {
          alert('清理失败: ' + (res.error || '未知错误'));
        }
      } catch (e) {
        alert('清理出错: ' + e.message);
      } finally {
        btn.disabled = false;
        btn.innerText = '清理冷却';
      }
    }
  };
});
```

---

## 验证方案

### 自动化/运行测试
1. 在本地启动 Web 控制台，验证能够正常登录进入管理面板。
2. 在“抢跑策略”面板上，查看策略 1、2、3 头部开关左侧是否正确渲染了“清理冷却”按钮，且样式与 UI 契合。
3. 点击“清理冷却”按钮，确认浏览器是否弹出确认框；点击“取消”应取消操作，点击“确认”应发送 API 请求并返回成功弹窗。
4. 检查后端 `data/cooldowns.json` 持久化文件，确认对应的键（以 `new-subnet`, `new-subnet-double`, `rename`, `coldkey-swap` 为前缀的键）已被成功删除。
5. 检查后端系统日志，确保输出 `[清理冷却] 清理了策略 [...] 的冷却...` 确认信息。
