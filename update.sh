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
echo -e "${CYAN}          Bittensor 套利与抢跑机器人一键升级向导 (Linux)${NC}"
echo -e "${PURPLE}=============================================================${NC}"
echo ""

# 1. 安全备份数据
echo -e "${YELLOW}>>> [第一步] 正在备份您的私有数据 (Wallets & Settings)...${NC}"
BACKUP_DIR="/tmp/taoli_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

if [ -d "data" ]; then
  cp -rp data/. "$BACKUP_DIR/"
  echo -e "${GREEN}备份成功！备份文件安全存储在: $BACKUP_DIR${NC}"
else
  echo -e "${YELLOW}未检测到已有的 data 目录，将跳过备份。${NC}"
fi
echo ""

# 2. 检查更新源并更新代码
echo -e "${YELLOW}>>> [第二步] 正在应用代码更新...${NC}"
if [ -d ".git" ]; then
  echo -e "${BLUE}检测到 Git 仓库，正在从 Git 远程仓库拉取最新代码...${NC}"
  git fetch --all
  
  # Check for uncommitted changes
  HAS_CHANGES=$(git status --porcelain)
  if [ -n "$HAS_CHANGES" ]; then
    echo -e "${YELLOW}检测到本地代码已被修改，正在暂存本地更改以防冲突...${NC}"
    git stash
  fi
  
  if git pull; then
    echo -e "${GREEN}代码拉取成功！${NC}"
  else
    echo -e "${RED}Git 拉取失败，请检查网络或冲突！${NC}"
  fi
  
  if [ -n "$HAS_CHANGES" ]; then
    echo -e "${BLUE}正在恢复您的本地代码修改...${NC}"
    git stash pop
  fi
else
  echo -e "${YELLOW}当前不是 Git 仓库。如果您是从压缩包或手动更新：${NC}"
  echo -e "请将新版本的文件直接覆盖至当前目录，然后运行本脚本以恢复配置并重启服务。${NC}"
  read -p "您是否已经手动覆盖了新代码文件？(y/N): " MANUAL_OVERWRITE
  if [ "$MANUAL_OVERWRITE" != "y" ] && [ "$MANUAL_OVERWRITE" != "Y" ]; then
    echo -e "${RED}更新已终止。请将新代码文件上传并覆盖至当前目录后重试。${NC}"
    exit 1
  fi
fi
echo ""

# 3. 恢复备份数据
echo -e "${YELLOW}>>> [第三步] 正在恢复备份的私有数据...${NC}"
mkdir -p data
if [ -d "$BACKUP_DIR" ] && [ "$(ls -A "$BACKUP_DIR" 2>/dev/null)" ]; then
  cp -rp "$BACKUP_DIR"/. data/
  chmod 600 data/settings.json data/wallets.json 2>/dev/null
  echo -e "${GREEN}私有数据（配置文件、钱包数据等）已成功恢复！${NC}"
else
  echo -e "${YELLOW}无备份数据需要恢复。${NC}"
fi
echo ""

# 4. 安装/更新依赖
echo -e "${YELLOW}>>> [第四步] 正在安装与更新 NPM 依赖包...${NC}"
npm install --production
echo -e "${GREEN}依赖包更新成功！${NC}"
echo ""

# 5. 代码语法自检
echo -e "${YELLOW}>>> [第五步] 正在对核心代码进行安全语法检测...${NC}"
if node --check server.js && node --check bot.js && node --check database.js; then
  echo -e "${GREEN}语法检测通过！代码未发现致命语法错误。${NC}"
else
  echo -e "${RED}警告: 语法检测未通过，新代码可能存在错误！请检查核心文件！${NC}"
  exit 1
fi
echo ""

# 6. 重启 PM2 守护进程
echo -e "${YELLOW}>>> [第六步] 正在重启机器人进程守护服务...${NC}"
if command -v pm2 &> /dev/null; then
  if pm2 describe bittensor-arbitrage-bot &> /dev/null; then
    pm2 restart bittensor-arbitrage-bot
    echo -e "${GREEN}PM2 守护进程 bittensor-arbitrage-bot 重启成功！${NC}"
  else
    echo -e "${BLUE}未发现正在运行的 PM2 守护进程，正在启动新进程...${NC}"
    pm2 start server.js --name bittensor-arbitrage-bot
    pm2 save
  fi
else
  echo -e "${YELLOW}未检测到 pm2 环境，如果您是在前台运行，请手动重启 node 进程。${NC}"
fi

echo ""
echo -e "${GREEN}=============================================================${NC}"
echo -e "${GREEN}             🎉 机器人系统一键升级完成！${NC}"
echo -e "${GREEN}=============================================================${NC}"
echo -e "数据已完整保留，服务已就绪并重启运行。"
echo -e "============================================================="
echo ""
exit 0
