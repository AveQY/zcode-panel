# ZCode Panel

面向 Linux 服务器的 **ZCode 网页对话控制台**。它将 `zcode app-server` 的会话能力以浏览器聊天界面呈现：新建与续接会话、工作区、模型/思考等级、会话归档、厂商配置和安全登录都通过同一个页面完成；**不提供通用 SSH 终端**。

> 代码中的 ZCode 对接已按社区 CLI `zcode-app-cli` 的 app-server 协议验证。请先在测试环境部署；暴露到公网前务必使用 HTTPS 和强登录密钥。

## 功能

- 原生 ZCode 会话：新建、打开历史、持续聊天、自动标题、工具调用和思考状态
- 历史消息分页加载：打开会话只拉**最近 50 条**，滚到顶部（或点顶部入口）继续按 50 条/页加载更早消息，插入后滚动位置原位保持不跳动；后端消息接口支持 `?limit=&before=<seq>` 分页参数并返回 `total/hasMore/oldestSeq`。大 JSON 响应（历史消息、静态资源）自动 gzip 压缩，静态资源带 12h 缓存头（版本号 `?v=` 控制更新），长会话秒开
- 会话后台执行：发出去的任务跑在服务端，**关掉网页/退出浏览器也继续执行到底**。服务端的事件拉取与浏览器连接无关；长任务里长时间没有事件（装依赖、构建、长 `sleep`）时会先向 app-server 求证是否仍在执行，不会把还在跑的任务误判成结束（另有 12 小时硬上限兜底）
- 多会话并行：会话之间互不影响。一个会话在跑，**照样可以切到别的会话继续用**（每个会话各自记执行状态，切换不会打断谁），侧栏用呼吸点标出正在后台执行的会话；后台跑完会通知，重开页面自动回到上次的会话并接回正在输出内容
- 只有「停止生成」能打断任务：除停止按钮外，任何操作都不会影响正在执行的任务——包括那些需要重启 app-server 的设置（会话归档/删除、厂商增删改、模型增删改、切换默认模型），此时会明确提示「还有 N 个会话正在执行」并拒绝执行，而不是把任务悄悄杀掉
- 工作区：选择已有工作区或新建，标注为“项目”或“任务”；左侧会话按工作区分组，组头可展开/折叠，折叠状态本地记忆
- 工作区管理：新增时可直接指定目录（名称不再被 slug 化改写）；支持重命名；支持移除，可选 `purge` 连同磁盘目录一起删除（需二次确认，工作区根目录禁止删除）
- 会话：左侧单击打开；右键或“⋯”菜单中归档、取消归档、彻底删除
- 彻底删除跑着的会话（停止并删除）：目标会话正在执行时，不带 force 的删除仍会被挡下（409，防误触）；确认弹窗会追问「停止并删除」，确认后走 `DELETE /api/sessions/{sid}?force=1`——先摘掉联动的巡检定时任务（绑定该会话时连 crontab 巡检行一起摘，防止删除窗口内被 cron 重新拉起），再停掉当前轮次、等 app-server 收尾（最多 20s，超时照删），然后 session/close + 删库 + 清扫磁盘遗留物。删除绑定会话会联动解绑巡检触发器，避免「会话删了定时任务还在」
- 聊天参数：模型、low/high/max 思考等级、中文模式（规划/构建/编辑/放行）；模型与思考等级在输入框左下，模式在右下，悬停显示模式说明
- 每个会话各用各的模型：会话模型独立保存，切换选择器即时生效且**只影响当前这个会话**（不重启 app-server、不影响别的会话）。会话正在执行时改动不打断它，登记为本轮跑完后的下一轮生效；侧栏每个会话都标着它用的模型（没单独设过的显示为跟随默认）
- 连续执行（输入框左下「连续」开关，按会话独立）：开启后一轮**正常收尾**会自动发续跑消息（默认「继续」，可自定义）开下一轮，最多连续 30 轮；一轮**以错误收场**（如模型 API 连不上、网络抖动）会自动重发同一条消息，最多重试 3 次（20s/60s/180s 退避）。**手动停止是总闸**：点「停止生成」立即关闭该会话的连续执行；手动发消息会重置连续计数。状态存在 `data/auto_run.json`，面板重启不丢；侧栏会话带「连续」标记
- 使用统计：只读汇总 ZCode CLI 的 `model_usage` 记录，设置中单独一栏（今日 / 近 7 天 / 近 30 天 / 全部，含 14 天柱状图与按模型明细）；页头右上角常驻今日 token 角标，用 k / m / b 缩写
- 模型定时（除「停止」之外唯一会主动打断任务的地方，且是用户自己设定的规则，会自动续跑）：按时段把**所有会话**切到指定模型。规则分单次 / 每日 / 每周，可设持续时长（到点自动切回原模型，0 = 一直生效直到手动恢复）。到点时的行为：先请求停止当前轮次（`session/stop`）、立刻把模型切到目标，然后**自动让任务接着做**（默认续跑指令「请继续执行任务。」可改）。实测 app-server 不保证能中断已经发出的模型请求，所以长生成/长工具一时停不下来时**不会把任务丢掉**：该会话会被登记成「待续跑」，一旦空闲就自动补发续跑提示（最多等 10 分钟），统计里也如实区分「立即续跑」和「待续跑」。生效期间顶栏横幅提示（含待续跑数量）、模型选择器锁定；改规则或重启服务后重新求值，不会漏执行
- 设置：通用设置、登录安全、模型配置、模型定时、使用统计；退出登录移至“登录安全”栏内
- Provider 管理：官方/自定义分栏；新增、编辑、删除第三方厂商及模型；API Key 默认掩码，眼睛按钮按需显示
- 厂商配置自带验证工具（都在弹窗里，**不改配置、不重启 app-server**）：
  - **测试**：每个模型后面都有「测试」按钮，真发一条最小对话请求，把结论说清楚——通了会回显耗时、实际请求的 URL 和模型的一小段回复；不通会区分是「路径不对 / Key 不对 / 额度不足 / 被限流 / 模型名不对 / 网络到不了」哪一类。若发现换成别的 Base URL 就能打到厂商，会直接给出该改成什么并支持一键填入
  - **获取模型列表**：从端点拉 `GET {base}/models`，勾选后批量加入模型配置，不用手抄模型 ID（手抄大小写/后缀最容易出错）
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
5. app-server 的连接是**可自愈**的：一个长会话在冷启动 `resume` 时会把整段历史作为**单行 JSON** 重放，长度容易超过 asyncio 默认的 64KB 单行上限，从而把读取协程打死（表现为 `Separator is found, but chunk is longer than limit`，进程还在但所有 RPC 超时）。本服务已把子进程的读缓冲上限提到 32MB，并在读取协程异常退出时标记连接失效、在无人执行任务时自动重启 app-server 恢复；`/healthz` 的 `appServer` 字段会如实反映连接是否可用。若仍遇到连接类报错，先看 `/healthz` 再重试。

### 排查：加完厂商/模型，一发消息就报错

协议对不上是最常见的一类，而且厂商报的错往往很难看懂。两种典型：

| 现象 | 真正原因 |
| --- | --- |
| `404 NOT_FOUND`（有的厂商会把它塞在 **HTTP 200** 的响应体里，例如智谱返回 `{"code":500,"msg":"404 NOT_FOUND"}`） | **接口类型与 Base URL 不匹配**。`OpenAI 兼容` 类型要求 Base URL 是 `…/v1` 这一级（服务会在后面拼 `/chat/completions`）；`Anthropic` 类型要求是 `…/api/anthropic` 这一级（拼 `/v1/messages`）。把 Anthropic 的地址填进 OpenAI 兼容类型，拼出来的路径就不存在 |
| `余额不足 / 无可用资源包`（`429`） | 路径其实对了，是**账号没有额度**。别再去改地址 |
| `模型不存在` | 模型 ID 抄错了（注意大小写与后缀，厂商真实 ID 通常全小写） |

弹窗里的「测试」和「获取模型列表」就是为这两件事准备的：先「获取模型列表」拉到厂商真实支持的模型 ID 再勾选；再对每个模型点「测试」。测试会依次尝试「当前配置的路径 → 已知厂商地址 → 补/去 `/v1` 的变体 → 另一种协议的路径」，命中哪个就把结论写出来，并可直接一键把 Base URL 改对。测试不会修改任何配置，也不会重启 app-server。

注意判定不看 HTTP 状态码：像上面智谱那种「200 里塞 404」的响应，只看状态码会把失败当成功，所以测试会一并解析响应体。

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

每次升级后至少验证：登录、左侧会话右键菜单、创建会话、一次短回复、一次含工具调用或长回复、设置中的模型列表、使用统计面板（图表峰值非 0）、工作区新建/改名/移除。

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
