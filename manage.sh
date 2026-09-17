#!/bin/bash
# ZCode Chat Console 管理工具（聊天版）
# 用法：manage.sh <命令> [参数]

set -u

BASE=/opt/zcode-chat
BAN_FILE=$BASE/data/banned.json
CONF=$BASE/config.json
SERVICE=zcode-chat

case "${1:-help}" in
  status)
    systemctl status "$SERVICE" --no-pager -l 2>&1 | head -16
    echo
    echo "--- 端口监听 ---"
    ss -lntp 2>/dev/null | grep -E ":8081\b" || echo "8081 未监听"
    echo
    echo "--- 健康检查 ---"
    curl -s -m 5 http://127.0.0.1:8081/healthz || echo "无响应"
    echo
    echo "--- app-server 进程 ---"
    pgrep -af "zcode app-server" || echo "app-server 未运行"
    ;;

  banned)
    python3 - "$BAN_FILE" <<'PYEOF'
import json, pathlib, sys, time
p = pathlib.Path(sys.argv[1])
try:
    data = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
except Exception as exc:
    print("读取失败:", exc)
    raise SystemExit(0)

if not data:
    print("名单为空，暂无任何记录")
    raise SystemExit(0)

print(f"{'IP 地址':<20} {'状态':<10} {'失败次数':<8} 最后尝试时间")
print("-" * 62)
for ip, rec in sorted(data.items()):
    state = "永久封禁" if rec.get("banned") else "仅记录"
    last = rec.get("last_fail_at")
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last)) if last else "-"
    print(f"{ip:<20} {state:<10} {rec.get('fails', 0):<8} {ts}")
PYEOF
    ;;

  unban)
    IP="${2:-}"
    if [ -z "$IP" ]; then
      echo "用法: manage.sh unban <ip>"
      exit 1
    fi
    python3 - "$BAN_FILE" "$IP" <<'PYEOF'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
data = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
ip = sys.argv[2]
if ip in data:
    data.pop(ip)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"已解除对 {ip} 的限制（服务会自动热加载）")
else:
    print(f"{ip} 不在名单中，无需处理")
PYEOF
    ;;

  unban-all)
    mkdir -p "$(dirname "$BAN_FILE")"
    echo '{}' > "$BAN_FILE"
    echo "已清空全部封禁记录（服务会自动热加载）"
    ;;

  sessions)
    echo "--- zcode 会话（最近 20 条，读自 zcode 自己的数据库）---"
    python3 - <<'PYEOF'
import sqlite3, time
db = "/root/.zcode/cli/db/db.sqlite"
conn = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "SELECT id, title, directory, time_updated, time_archived FROM session "
    "ORDER BY time_updated DESC LIMIT 20"
).fetchall()
if not rows:
    print("暂无会话")
for r in rows:
    ts = time.strftime("%m-%d %H:%M", time.localtime((r["time_updated"] or 0) / 1000))
    flag = " [已归档]" if r["time_archived"] else ""
    print("  %s  %s  %s%s" % (ts, r["id"], (r["title"] or "(无标题)")[:36], flag))
tot = conn.execute("SELECT COUNT(*) FROM session").fetchone()[0]
arc = conn.execute("SELECT COUNT(*) FROM session WHERE time_archived IS NOT NULL").fetchone()[0]
print("共 %d 个会话，其中已归档 %d 个" % (tot, arc))
PYEOF
    ;;

  workspaces)
    echo "--- 控制台登记的工作区 ---"
    python3 - "$BASE/data/workspaces.json" <<'PYEOF'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
if not p.exists():
    print("还没有登记任何工作区")
    raise SystemExit(0)
items = (json.loads(p.read_text(encoding="utf-8")) or {}).get("workspaces") or []
if not items:
    print("还没有登记任何工作区")
for it in items:
    print("  [%s] %-20s %s" % ("任务" if it.get("purpose") == "task" else "项目",
                               it.get("name"), it.get("path")))
print("共 %d 个工作区" % len(items))
PYEOF
    ;;

  providers)
    echo "--- 模型厂商配置（读自 ~/.zcode/cli/config.json）---"
    python3 - <<'PYEOF'
import json, pathlib
p = pathlib.Path("/root/.zcode/cli/config.json")
cfg = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
main = (cfg.get("model") or {}).get("main") or ""
for pid, prov in (cfg.get("provider") or {}).items():
    opts = prov.get("options") or {}
    key = opts.get("apiKey") or ""
    masked = (key[:4] + "****" + key[-4:]) if len(key) > 8 else ("*" * len(key))
    print("  [%s] %s  (%s)" % (pid, prov.get("name") or pid, prov.get("kind")))
    print("       Base URL: %s" % (opts.get("baseURL") or "-"))
    print("       API Key : %s" % (masked or "-"))
    for mid, m in (prov.get("models") or {}).items():
        star = " ★默认" if main == "%s/%s" % (pid, mid) else ""
        print("         - %s (%s)%s" % (mid, m.get("name") or mid, star))
PYEOF
    ;;

  running)
    echo "--- 服务端记录的正在执行的对话 ---"
    curl -s -m 5 http://127.0.0.1:8081/healthz | python3 -c \
      "import json,sys; d=json.load(sys.stdin); print('  正在执行:', d.get('running'), '个')" 2>/dev/null \
      || echo "无法读取"
    echo "--- app-server 进程 ---"
    pgrep -af "zcode app-server" || echo "app-server 未运行"
    ;;

  kill)
    PID="${2:-}"
    if [ -z "$PID" ]; then
      echo "用法: manage.sh kill <pid>（pid 可用 manage.sh running 查看）"
      exit 1
    fi
    kill "$PID" && echo "已终止进程 $PID（服务会自动拉起新的 app-server）"
    ;;

  key)
    NEWKEY="${2:-}"
    if [ -z "$NEWKEY" ]; then
      echo "用法: manage.sh key <新密钥>"
      exit 1
    fi
    python3 - "$CONF" "$NEWKEY" <<'PYEOF'
import hashlib, json, pathlib, sys
conf_path, newkey = sys.argv[1], sys.argv[2]
p = pathlib.Path(conf_path)
cfg = json.loads(p.read_text(encoding="utf-8"))
cfg["key_sha256"] = hashlib.sha256(newkey.encode("utf-8")).hexdigest()
# key_plain 只用于设置页回显；不写的话设置页会显示成默认值
cfg["key_plain"] = newkey
p.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
print("密钥已更新，执行 manage.sh restart 生效")
PYEOF
    ;;

  log)
    journalctl -u "$SERVICE" -n "${2:-60}" --no-pager
    ;;

  restart)
    systemctl restart "$SERVICE" && sleep 3 && systemctl is-active "$SERVICE"
    ;;

  help|*)
    cat <<'HELP'
ZCode Chat Console 管理工具（聊天版）

  manage.sh status         查看服务状态、端口、健康检查与 app-server 进程
  manage.sh banned         查看失败记录与永久封禁名单
  manage.sh unban <ip>     解除指定 IP 的限制
  manage.sh unban-all      清空全部封禁记录
  manage.sh sessions       列出 zcode 会话（含归档标记）
  manage.sh workspaces     列出控制台登记的工作区
  manage.sh providers      列出模型厂商与模型（密钥打码）
  manage.sh running        查看正在执行的对话与 app-server 进程
  manage.sh kill <pid>     终止指定进程
  manage.sh key <新密钥>    更换访问密钥
  manage.sh log [行数]     查看服务日志
  manage.sh restart        重启服务

说明：
  - 本控制台只暴露 zcode 的对话能力，不提供服务器 shell。
  - 对话与流式走 ZCode Protocol（zcode app-server）；历史消息直接读 zcode 的 SQLite。
  - 归档/删除会话会直接改库，并重启 app-server 让它重新加载。
  - 封禁记录文件：/opt/zcode-chat/data/banned.json（服务会按 mtime 自动热加载）
  - 工作区登记：  /opt/zcode-chat/data/workspaces.json
  - 配置文件：    /opt/zcode-chat/config.json
  - zcode 配置：  /root/.zcode/cli/config.json（控制台设置页也改这个）
HELP
    ;;
esac
