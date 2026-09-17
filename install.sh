#!/usr/bin/env bash
# ZCode Panel 部署脚本：在已满足 README 前置条件的 Linux 服务器上执行。
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/zcode-chat}"
SERVICE_NAME="${SERVICE_NAME:-zcode-chat}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${EUID}" -ne 0 ]]; then
  echo "请以 root 执行：sudo bash install.sh"
  exit 1
fi

if ! command -v zcode >/dev/null 2>&1; then
  echo "未找到 zcode。请先按 README 安装并配置 zcode-app-cli。"
  exit 1
fi
if ! zcode version >/dev/null 2>&1; then
  echo "zcode 命令无法运行，请先完成 ZCode CLI 配置。"
  exit 1
fi
if ! python3 -c 'import aiohttp' >/dev/null 2>&1; then
  echo "缺少 aiohttp。请先执行：python3 -m pip install -r requirements.txt"
  exit 1
fi

mkdir -p "${APP_DIR}" "${APP_DIR}/data"
install -m 755 "${SCRIPT_DIR}/app.py" "${APP_DIR}/app.py"
install -m 755 "${SCRIPT_DIR}/manage.sh" "${APP_DIR}/manage.sh"
rm -rf "${APP_DIR}/static"
cp -a "${SCRIPT_DIR}/static" "${APP_DIR}/static"

# 初次安装生成无密钥配置：应用默认登录密钥为 admin123。
# 生产环境必须立刻用 manage.sh key 改为强密钥。
if [[ ! -f "${APP_DIR}/config.json" ]]; then
  install -m 600 "${SCRIPT_DIR}/config.example.json" "${APP_DIR}/config.json"
fi

install -m 644 "${SCRIPT_DIR}/systemd/zcode-chat.service" "/etc/systemd/system/${SERVICE_NAME}.service"
systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"

printf '\n部署完成。\n'
printf '健康检查：curl -s http://127.0.0.1:8081/healthz\n'
printf '立即设置登录密钥：%s/manage.sh key <强密钥>\n' "${APP_DIR}"
printf '查看状态：%s/manage.sh status\n' "${APP_DIR}"
