#!/usr/bin/env bash

# Terminal Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

clear
echo -e "${PURPLE}=============================================================${NC}"
echo -e "${CYAN}          Bittensor 套利与抢跑机器人一键安装向导${NC}"
echo -e "${PURPLE}=============================================================${NC}"
echo -e "系统环境要求: Ubuntu 20.04 或以上 (以 root 权限或 sudo 运行)"
echo ""

# Check if script is run as root
if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}警告: 请以 sudo 运行此脚本，以确保安装核心依赖包成功！${NC}"
  echo "例如: sudo bash install.sh"
  exit 1
fi

# ----------------- 1. 交互式安装向导 -----------------
echo -e "${YELLOW}>>> [第一步] 请输入基础系统配置${NC}"
echo -e "------------------------------------"

# A. Port selection
DEFAULT_PORT=8080
read -p "1. 请设置控制台运行端口 (默认: $DEFAULT_PORT): " PORT
if [ -z "$PORT" ]; then
  PORT=$DEFAULT_PORT
fi

# Check if port is already in use
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null ; then
  echo -e "${RED}错误: 端口 $PORT 已被其他程序占用，请输入其他端口！${NC}"
  exit 1
fi
echo -e "${GREEN}使用端口: $PORT${NC}"
echo ""

# B. Admin Username
DEFAULT_USER="admin"
read -p "2. 请设置控制面板管理员账号 (默认: $DEFAULT_USER): " USERNAME
if [ -z "$USERNAME" ]; then
  USERNAME=$DEFAULT_USER
fi
echo -e "${GREEN}使用管理员账号: $USERNAME${NC}"
echo ""

# C. Admin Password
read -sp "3. 请设置控制面板管理员密码 (最少 6 位): " PASSWORD
echo ""
while [ ${#PASSWORD} -lt 6 ]; do
  echo -e "${RED}密码强度不足，请重新输入（最少 6 位）！${NC}"
  read -sp "请设置控制面板管理员密码: " PASSWORD
  echo ""
done

# Confirm password
read -sp "请再次输入密码以确认: " PASSWORD_CONFIRM
echo ""
while [ "$PASSWORD" != "$PASSWORD_CONFIRM" ]; do
  echo -e "${RED}两次输入的密码不一致，请重新输入！${NC}"
  read -sp "请再次确认密码: " PASSWORD_CONFIRM
  echo ""
done
echo -e "${GREEN}密码已成功设置。${NC}"
echo ""

# ----------------- 2. 系统依赖包安装 -----------------
echo -e "${YELLOW}>>> [第二步] 正在安装系统环境与核心依赖包...${NC}"
echo -e "------------------------------------"

# Update package repository
apt-get update -y

# Check Node.js
if ! command -v node &> /dev/null; then
  echo -e "${BLUE}未检测到 Node.js，正在从 NodeSource 获取 Node.js v20.x...${NC}"
  curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
  apt-get install -y nodejs
else
  NODE_VERSION=$(node -v)
  echo -e "${GREEN}检测到 Node.js 已安装，版本: $NODE_VERSION${NC}"
fi

# Install pm2
if ! command -v pm2 &> /dev/null; then
  echo -e "${BLUE}正在安装 pm2 进程守护程序...${NC}"
  npm install -g pm2
else
  echo -e "${GREEN}检测到 pm2 已安装。${NC}"
fi

# Install local dependencies
echo -e "${BLUE}正在安装机器人依赖库 (npm install)...${NC}"
npm install --production

# Create database data folder
mkdir -p data
chmod 700 data

# ----------------- 3. 生成加密配置文件 -----------------
echo -e "${YELLOW}>>> [第三步] 正在生成本地配置文件并计算密码哈希...${NC}"
echo -e "------------------------------------"

# Safely hand over credentials using environment variables to avoid shell escaping exploits
PORT=$PORT USERNAME=$USERNAME PASSWORD=$PASSWORD node -e "
const crypto = require('crypto');
const fs = require('fs');

const password = process.env.PASSWORD;
const username = process.env.USERNAME;
const port = parseInt(process.env.PORT, 10);

const salt = crypto.randomBytes(16).toString('hex');
const hash = crypto.pbkdf2Sync(password, salt, 1000, 64, 'sha256').toString('hex');

const config = {
  webPort: port,
  webUser: username,
  webPassHash: hash,
  webPassSalt: salt,
  primaryNode: 'ws://127.0.0.1:9944',
  backupNode: 'wss://entrypoint-finney.opentensor.ai:443',
  rateLimitPerSec: 10,
  telegramEnabled: false,
  telegramToken: '',
  telegramChatId: '',
  dashingEnabled: true,
  dashingAmount: 100,
  dashingRetries: 10,
  dashingIntervalMs: 1000,
  dashingTimeoutMs: 30000,
  renameEnabled: true,
  swapEnabled: true
};

fs.writeFileSync('data/settings.json', JSON.stringify(config, null, 2), 'utf8');
"

# Ensure wallets.json exists
if [ ! -f data/wallets.json ]; then
  echo "[]" > data/wallets.json
fi

chmod 600 data/settings.json data/wallets.json
echo -e "${GREEN}加密配置文件已生成成功，密码已以 PBKDF2 哈希存储在 settings.json！${NC}"
echo ""

# ----------------- 4. 防火墙端口开放 -----------------
echo -e "${YELLOW}>>> [第四步] 正在配置系统防火墙端口...${NC}"
echo -e "------------------------------------"

if command -v ufw &> /dev/null; then
  echo -e "${BLUE}检测到 ufw 防火墙，正在放行端口 $PORT/tcp...${NC}"
  ufw allow $PORT/tcp
  ufw reload
  echo -e "${GREEN}防火墙放行完成。${NC}"
else
  echo -e "${YELLOW}未检测到 ufw 防火墙，请确认云服务器安全组已放行 TCP 端口 $PORT ！${NC}"
fi
echo ""

# ----------------- 5. 启动服务与配置自启 -----------------
echo -e "${YELLOW}>>> [第五步] 正在启动机器人控制台进程...${NC}"
echo -e "------------------------------------"

# Stop existing if running
pm2 stop bittensor-arbitrage-bot &> /dev/null
pm2 delete bittensor-arbitrage-bot &> /dev/null

# Start daemon
pm2 start server.js --name bittensor-arbitrage-bot
pm2 save

# Setup startup
echo -e "${BLUE}正在配置 pm2 开机自启动守护环境...${NC}"
pm2 startup | tail -n 1 | bash
pm2 save

# ----------------- 6. 状态汇总与结束 -----------------
# Get public IP address
PUBLIC_IP=$(curl -s https://ipinfo.io/ip)
if [ -z "$PUBLIC_IP" ]; then
  PUBLIC_IP="您的服务器公网IP"
fi

echo ""
echo -e "${GREEN}=============================================================${NC}"
echo -e "${GREEN}             🎉 机器人与可视化控制台安装成功！${NC}"
echo -e "${GREEN}=============================================================${NC}"
echo -e "1. 控制面板登录地址: ${CYAN}http://$PUBLIC_IP:$PORT${NC}"
echo -e "2. 管理员账号: ${YELLOW}$USERNAME${NC}"
echo -e "3. 管理员密码: ${YELLOW}(您刚才设置的密码)${NC}"
echo -e "-------------------------------------------------------------"
echo -e "🔧 常用管理维护命令:"
echo -e "  - 查看机器人状态/重启情况: ${BLUE}pm2 list${NC}"
echo -e "  - 查看实时运行控制台日志: ${BLUE}pm2 logs bittensor-arbitrage-bot${NC}"
echo -e "  - 手动重启整个控制台与服务: ${BLUE}pm2 restart bittensor-arbitrage-bot${NC}"
echo -e "  - 停止整个控制台服务: ${BLUE}pm2 stop bittensor-arbitrage-bot${NC}"
echo -e "============================================================="
echo ""
exit 0
