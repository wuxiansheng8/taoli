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
  echo -e "${BLUE}检测到 Git 仓库，正在获取远程最新版本...${NC}"
  CURRENT_BRANCH=$(git branch --show-current 2>/dev/null || true)
  if [ -z "$CURRENT_BRANCH" ]; then
    CURRENT_BRANCH="main"
  fi
  REMOTE_REF="origin/$CURRENT_BRANCH"
  OLD_UPDATE_HASH=$(git rev-parse HEAD:update.sh 2>/dev/null || true)

  if ! git fetch --all --prune; then
    echo -e "${RED}Git fetch 失败，更新已终止，未重启服务。${NC}"
    exit 1
  fi
  if ! git rev-parse --verify "$REMOTE_REF" >/dev/null 2>&1; then
    echo -e "${YELLOW}未找到远程分支 $REMOTE_REF，自动回退使用 origin/main。${NC}"
    REMOTE_REF="origin/main"
  fi
  
  # Check for uncommitted changes
  HAS_CHANGES=$(git status --porcelain)
  if [ -n "$HAS_CHANGES" ]; then
    echo -e "${YELLOW}检测到本地文件已被修改（您的钱包数据和配置在 data 目录中，已被 git 忽略，安全不会丢失）。${NC}"
    echo -n -e "${CYAN}是否强制清除本地修改并更新到最新版？(输入 y 强制覆盖并更新，输入其他键尝试保留修改升级) [y/N]: ${NC}"
    read -r FORCE_UPDATE
    if [ "$FORCE_UPDATE" = "y" ] || [ "$FORCE_UPDATE" = "Y" ]; then
      echo -e "${BLUE}正在执行强制更新 (git reset --hard + git clean -fd)...${NC}"
      if ! git reset --hard "$REMOTE_REF"; then
        echo -e "${RED}Git reset 失败，更新已终止，未重启服务。${NC}"
        exit 1
      fi
      git clean -fd
      echo -e "${GREEN}本地修改已清理，代码已重置到最新版本！${NC}"
    else
      echo -e "${YELLOW}正在尝试暂存本地更改以防冲突...${NC}"
      if ! git stash push -u -m "taoli-auto-update-$(date +%Y%m%d_%H%M%S)"; then
        echo -e "${RED}Git stash 失败，更新已终止，未重启服务。${NC}"
        exit 1
      fi
      if git pull; then
        echo -e "${GREEN}代码拉取成功！${NC}"
      else
        echo -e "${RED}Git 拉取失败，请检查网络或冲突！更新已终止，未重启服务。${NC}"
        exit 1
      fi
      echo -e "${BLUE}正在尝试恢复您的本地修改...${NC}"
      if ! git stash pop; then
        echo -e "${RED}恢复本地修改失败，存在冲突。请手动解决，或重新运行本脚本选择强制覆盖。服务未重启。${NC}"
        exit 1
      fi
    fi
  else
    if git pull; then
      echo -e "${GREEN}代码拉取成功！${NC}"
    else
      echo -e "${RED}Git 拉取失败，请检查网络或冲突！更新已终止，未重启服务。${NC}"
      exit 1
    fi
  fi

  NEW_UPDATE_HASH=$(git rev-parse HEAD:update.sh 2>/dev/null || true)
  if [ -n "$OLD_UPDATE_HASH" ] && [ -n "$NEW_UPDATE_HASH" ] && [ "$OLD_UPDATE_HASH" != "$NEW_UPDATE_HASH" ] && [ "${TAOLI_UPDATE_REEXEC:-0}" != "1" ]; then
    chmod +x ./update.sh 2>/dev/null || true
    echo -e "${YELLOW}检测到 update.sh 自身已更新，正在切换到新版脚本重新执行，确保本次更新逻辑生效...${NC}"
    exec env TAOLI_UPDATE_REEXEC=1 bash ./update.sh
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
  chmod 600 data/settings.json data/wallets.json data/.key 2>/dev/null
  echo -e "${GREEN}私有数据（配置文件、钱包数据等）已成功恢复！${NC}"
else
  echo -e "${YELLOW}无备份数据需要恢复。${NC}"
fi
echo ""

# 4. 安装/更新依赖
echo -e "${YELLOW}>>> [第四步] 正在安装与更新 NPM 依赖包...${NC}"
if ! npm install --omit=dev; then
  echo -e "${RED}NPM 依赖安装失败，更新已终止，未重启服务。${NC}"
  exit 1
fi
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
  # 动态检测当前工作目录下已注册的 PM2 进程名
  export CURRENT_DIR=$(pwd)
  DETECTED_PM2_INFO=$(node -e "
  const execSync = require('child_process').execSync;
  const path = require('path');
  try {
    const list = JSON.parse(execSync('pm2 jlist').toString());
    const match = list.find(p => {
      const env = p.pm2_env || {};
      const cwd = env.pm_cwd || env.cwd;
      return cwd && path.resolve(cwd) === path.resolve(process.env.CURRENT_DIR);
    });
    if (match) console.log(String(match.pm_id) + '|' + match.name);
  } catch (e) {}
  " 2>/dev/null)
  DETECTED_PM2_ID=""
  DETECTED_PM2_NAME=""
  if [ -n "$DETECTED_PM2_INFO" ]; then
    DETECTED_PM2_ID="${DETECTED_PM2_INFO%%|*}"
    DETECTED_PM2_NAME="${DETECTED_PM2_INFO#*|}"
  fi

  # 主动检查并释放可能被残留/孤儿进程占用的 Web 端口，防止启动冲突
  PORT=$(node -e "
  const fs = require('fs');
  try {
    const cfg = JSON.parse(fs.readFileSync('data/settings.json', 'utf8'));
    console.log(cfg.webPort || 8080);
  } catch(e) { console.log(8080); }
  " 2>/dev/null)
  
  if command -v lsof >/dev/null 2>&1; then
    PIDS=$(lsof -tiTCP:$PORT -sTCP:LISTEN || true)

    if [ -n "$PIDS" ]; then
      echo -e "${YELLOW}检测到端口 $PORT 被以下进程占用：${NC}"
      lsof -iTCP:$PORT -sTCP:LISTEN || true

      for PID in $PIDS; do
        CMD=$(ps -p "$PID" -o comm= 2>/dev/null || true)
        ARGS=$(ps -p "$PID" -o args= 2>/dev/null || true)

        if [ -n "$DETECTED_PM2_ID" ] && pm2 pid "$DETECTED_PM2_ID" 2>/dev/null | grep -qx "$PID"; then
          echo -e "${BLUE}端口由当前 PM2 管理的机器人进程占用，稍后将通过 PM2 restart 正常重启：PID=$PID${NC}"
          continue
        fi

        if echo "$ARGS" | grep -q "server.js"; then
          echo -e "${YELLOW}检测到疑似旧机器人孤儿进程 PID=$PID，先尝试正常结束...${NC}"
          kill "$PID" 2>/dev/null || true
          sleep 2

          if kill -0 "$PID" 2>/dev/null; then
            echo -e "${RED}进程仍未退出，强制结束 PID=$PID...${NC}"
            kill -9 "$PID" 2>/dev/null || true
          fi
        else
          echo -e "${BLUE}端口占用进程不是 server.js，跳过强杀：PID=$PID CMD=$CMD${NC}"
        fi
      done
    fi
  else
    echo -e "${YELLOW}未安装 lsof，跳过端口占用自动释放。${NC}"
  fi
  
  if [ -n "$DETECTED_PM2_ID" ]; then
    echo -e "${GREEN}检测到当前目录 PM2 进程: $DETECTED_PM2_NAME (id: $DETECTED_PM2_ID)，正在执行真正重启...${NC}"
    if ! pm2 restart "$DETECTED_PM2_ID" --update-env; then
      echo -e "${RED}PM2 重启失败，更新已终止。${NC}"
      exit 1
    fi
    pm2 save
  else
    if pm2 describe bittensor-arbitrage-bot >/dev/null 2>&1; then
      echo -e "${YELLOW}检测到已有名为 bittensor-arbitrage-bot 的标准 PM2 进程，正在执行真正重启...${NC}"
      if ! pm2 restart bittensor-arbitrage-bot --update-env; then
        echo -e "${RED}PM2 重启失败，更新已终止。${NC}"
        exit 1
      fi
    else
      echo -e "${BLUE}正在创建并启动新 PM2 进程 'bittensor-arbitrage-bot'...${NC}"
      if ! pm2 start server.js --name bittensor-arbitrage-bot; then
        echo -e "${RED}PM2 启动失败，更新已终止。${NC}"
        exit 1
      fi
    fi
    pm2 save
  fi
  echo -e "${GREEN}PM2 进程守护服务重启并重载成功！${NC}"
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
