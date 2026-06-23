const axios = require('axios');

// 全局告警冷却时间戳 (用于全局防抖)
let lastAlertTime = 0;

/**
 * 安全脱敏错误消息中的敏感 URL 信息
 */
function sanitizeErrorMessage(msg) {
  if (!msg) return '';
  // 屏蔽 Webhook 链接中的 integration_key
  return msg.replace(/integration_key=[a-zA-Z0-9_-]+/gi, 'integration_key=******');
}

/**
 * 发送打新/抢跑告警事件，触发 FlashDuty 电话呼叫
 * @param {string} title 告警标题
 * @param {string} description 告警详情
 * @param {object} settings 已经读取好的系统 settings 配置项
 * @param {function} logFn bot.js 传入的流式日志函数
 */
async function sendAlert(title, description, settings, logFn) {
  if (!settings || !settings.flashDutyEnabled || !settings.flashDutyWebhookUrl) {
    return;
  }
  
  const now = Date.now();
  const cooldownMs = settings.flashDutyCooldownMs !== undefined ? Number(settings.flashDutyCooldownMs) : 300000;

  if (now - lastAlertTime < cooldownMs) {
    if (typeof logFn === 'function') {
      const cooldownMins = Math.ceil(cooldownMs / 60000);
      logFn('INFO', `[FlashDuty] 处于 ${cooldownMins} 分钟冷却中，已跳过本次电话告警: ${title}`);
    }
    return;
  }

  // 立即占位，防止高并发穿透
  lastAlertTime = now;

  const payload = {
    title_rule: title,
    event_status: 'Critical',
    description: description,
    labels: {
      service: 'taoli',
      event: 'flashduty_alert'
    }
  };

  try {
    await axios.post(settings.flashDutyWebhookUrl, payload, { timeout: 10000 });
    if (typeof logFn === 'function') {
      logFn('SUCCESS', `[FlashDuty] 电话告警事件发送成功: ${title}`);
    }
  } catch (err) {
    const rawError = err.message || String(err);
    const safeError = sanitizeErrorMessage(rawError);
    if (typeof logFn === 'function') {
      logFn('WARN', `[FlashDuty] 电话告警发送失败: ${safeError}`);
    }
    // 如果发送失败，重置时间戳，允许立即重发
    lastAlertTime = 0;
  }
}

/**
 * 发送测试告警 (用于管理页面测试按钮)
 */
async function sendTestAlert(webhookUrl) {
  const payload = {
    title_rule: "TAOLI 测试电话告警",
    event_status: "Critical",
    description: "这是 TAOLI 的 FlashDuty 测试电话告警",
    labels: {
      service: "taoli",
      event: "flashduty_test"
    }
  };

  try {
    await axios.post(webhookUrl, payload, { timeout: 10000 });
    return { success: true };
  } catch (err) {
    const rawError = err.response?.data?.message || err.message;
    const safeError = sanitizeErrorMessage(rawError);
    return { success: false, error: safeError };
  }
}

module.exports = {
  sendAlert,
  sendTestAlert
};
