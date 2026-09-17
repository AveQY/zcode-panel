# ZCode Panel

面向 Linux 服务器的 **ZCode 网页对话控制台**。它将 `zcode app-server` 的会话能力以浏览器聊天界面呈现：新建与续接会话、工作区、模型/思考等级、会话归档、厂商配置和安全登录都通过同一个页面完成；**不提供通用 SSH 终端**。

> 代码中的 ZCode 对接已按社区 CLI `zcode-app-cli` 的 app-server 协议验证。请先在测试环境部署；暴露到公网前务必使用 HTTPS 和强登录密钥。

## 功能

- 原生 ZCode 会话：新建、打开历史、持续聊天、自动标题、工具调用和思考状态
- 工作区：选择已有工作区或新建，标注为“项目”或“任务”
- 会话：左侧单击打开；右键或“⋯”菜单中归档、取消归档、彻底删除
- 聊天参数：模型、low/high/max 思考等级、中文模式（规划/构建/编辑/放行）及说明
- 设置：通用设置、登录安全、模型配置三栏布局
- Provider 管理：官方/自定义分栏；新增、编辑、删除第三方厂商及模型；API Key 默认掩码，眼睛按钮按需显示
- 安全：登录密钥哈希校验、连续失败 5 次按来源 IP 永久封禁、无 shell HTTP 接口

## 已验证版本与前置条件

| 项目 | 要求 / 已验证版本 | 说明 |
| --- | --- | --- |
| 操作系统 | 64 位 Linux，建议 Ubuntu 22.04+ / Debian 12+ | 当前路径和 systemd 配置以 Linux 为目标 |
| 权限 | `root`（或将 app.py 中 `/root/.zcode` 路径改为服务账户路径） | 默认读取 root 的 ZCode 数据库、配置与工作区 |
| Python | Python 3.10+；已验证 Python 3.10 | 用于 aiohttp Web 服务 |
| Python 依赖 | `aiohttp==3.14.3` | 见 `requirements.txt` |
| Node.js | Node.js 22+（由 ZCode CLI 自带/依赖） | 用于运行 ZCode app-server |
| ZCode CLI | `zcode-app-cli 3.11.2-25`，runtime `0.16.5` | **社区 CLI，非官方桌面端**；这是当前已完整验证的组合 |
| ZCode 配置 | `/root/.zcode/cli/config.json` 且至少有一个可用 provider/model | 先独立确认 `zcode app-server` 能启动、模型能调用 |
| 端口 | 默认 TCP 8081 | 公网映射、反向代理或防火墙由部署者配置 |

### 重要限制

1. 该项目不是官方 ZCode 服务，也不是通用远程终端；它是对 CLI 的 Web 聊天封装。
2. 目前代码默认使用 `/usr/local/bin/zcode`、`/root/.zcode/cli/db/db.sqlite` 与 `/root/.zcode/cli/config.json`。如果用非 root 用户部署，需要先修改 `app.py` 的路径常量及 systemd 的 `User`/`HOME`。
3. 只用 HTTP 会让登录密钥以明文经过网络。公网部署必须放在 Nginx/Caddy 等 HTTPS 反向代理之后，或通过 VPN / 内网访问。
4. 失败封禁按服务端看到的来源 IP 计算。若反向代理/NAT 没有保留真实 IP，可能误伤同一出口用户；仅在可信代理下设置 `trust_proxy=true`。

## 安装

### 1. 安装并先验证 ZCode CLI

本仓库**不自动安装或写入 ZCode API Key**。先使用你的既有方式安装并配置 `zcode-app-cli`，然后至少验证：

```bash
zcode version
# 期望看到 zcode-app-cli 3.11.2-25 / zcode-runtime 0.16.5（或经你自己回归验证的兼容版本）
zcode app-server
# 看到进程启动即可 Ctrl+C；正式服务会负责拉起它
```

确认 provider 配置在服务账户的 `~/.zcode/cli/config.json` 中可用。不要把该文件、API Key 或任何 provider 密钥提交到 Git。

### 2. 获取仓库并安装 Python 依赖

```bash
git clone https://gitee.com/aweqy/zcode-pannel.git
cd zcode-pannel
python3 -m pip install -r requirements.txt
```

### 3. 部署 systemd 服务

```bash
sudo bash install.sh

# 初始状态没有自定义登录密钥时，应用回退为 admin123。
# 必须立即改成自己的强密钥：
sudo /opt/zcode-chat/manage.sh key '请替换为至少 6 位的强密钥'

curl -s http://127.0.0.1:8081/healthz
sudo /opt/zcode-chat/manage.sh status
```

安装脚本的行为：

- 将源码复制到 `/opt/zcode-chat/`
- 初次安装时由 `config.example.json` 生成 `/opt/zcode-chat/config.json`（权限 600）
- 保留已经存在的 `config.json`，因此再次运行不会覆盖登录密钥或运行时设置
- 安装并启用 `zcode-chat.service`

> 脚本会替换 `/opt/zcode-chat/static/`、`app.py`、`manage.sh` 以及同名 systemd unit。升级前建议先备份：`sudo tar czf /opt/zcode-chat-backup-$(date +%F).tar.gz /opt/zcode-chat`。

## 配置

首次部署配置文件为 `/opt/zcode-chat/config.json`。不要提交真实文件；仓库只提供 `config.example.json`。

| 字段 | 默认 | 用途 |
| --- | --- | --- |
| `port` / `bind` | `8081` / `0.0.0.0` | Web 服务监听地址 |
| `key_sha256` / `key_plain` | 空 | 登录密钥的哈希与设置页回显字段；由 `manage.sh key` 或设置页维护 |
| `max_attempts` | `5` | 失败次数上限，到达后永久封禁 IP |
| `token_ttl` | `43200` | 登录令牌有效期，单位秒 |
| `default_mode` | `build` | 默认聊天模式 |
| `auto_approve` | `true` | app-server 权限请求的默认放行策略 |
| `workspace_root` | `/root` | 允许新建工作区的根目录 |
| `trust_proxy` | `false` | 仅可信反向代理场景才可开启 |

## 日常运维

```bash
sudo /opt/zcode-chat/manage.sh status
sudo /opt/zcode-chat/manage.sh sessions
sudo /opt/zcode-chat/manage.sh workspaces
sudo /opt/zcode-chat/manage.sh providers
sudo /opt/zcode-chat/manage.sh banned
sudo /opt/zcode-chat/manage.sh unban <IP>
sudo /opt/zcode-chat/manage.sh key '<新密钥>'
sudo /opt/zcode-chat/manage.sh log 100
sudo /opt/zcode-chat/manage.sh restart

# systemd 原生命令
sudo systemctl status zcode-chat
sudo journalctl -u zcode-chat -f
```

## 升级

```bash
cd zcode-pannel
git pull
python3 -m pip install -r requirements.txt
sudo bash install.sh
sudo systemctl status zcode-chat --no-pager
curl -s http://127.0.0.1:8081/healthz
```

每次升级后至少验证：登录、左侧会话右键菜单、创建会话、一次短回复、一次含工具调用或长回复、设置中的模型列表。

## 开发验证

```bash
# Python / 前端语法
python3 -m py_compile app.py
node --check static/app.js

# 线上版本可使用项目外独立的 aiohttp/CDP 回归脚本验证；
# 不要将真实登录密钥、ZCode 配置或会话数据库纳入仓库。
```

## 项目结构

```text
.
├── app.py                         # aiohttp 后端 + ZCode app-server 协议适配
├── static/                        # 无构建步骤的浏览器前端
├── manage.sh                      # 状态、日志、封禁、会话、provider 运维命令
├── config.example.json            # 不含密钥的配置样例
├── requirements.txt               # Python 依赖
├── install.sh                     # /opt + systemd 部署脚本
└── systemd/zcode-chat.service     # systemd unit
```
