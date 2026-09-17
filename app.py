#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZCode 对话控制台 —— 后端

架构：
    浏览器 ──WebSocket──> 本服务 ──stdio(JSON)──> zcode app-server
                            │
                            └── SQLite（只读会话历史 / 归档删除）

关键点：
  * 对话与流式走 **ZCode Protocol**（`zcode app-server`），能拿到真正的逐字流式
    （`model.streaming` / `kind=text_delta`）、工具调用、自动标题、用量统计。
  * 事件是**拉取式**的：`session/subscribe` 建立订阅拿到游标，
    再用 `session/events {afterSeq}` 按 seq 拉增量。用 seq 游标所以不丢事件。
  * 会话历史仍然直接读 zcode 的 SQLite（格式稳定、已验证）。
  * 归档/删除没有协议方法，直接改库；改完重启 app-server 让它重新读。
  * app-server 会**反向请求客户端**（运行时偏好、权限确认等），必须及时应答，
    否则它 15 秒超时并让整个请求失败。

安全模型沿用：密钥认证 + 失败次数超限按 IP 永久封禁。
"""

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import signal
import sqlite3
import time
from pathlib import Path

from aiohttp import web, WSMsgType

# ---------------------------------------------------------------- 路径与常量

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
CONFIG_PATH = BASE_DIR / "config.json"
BAN_PATH = DATA_DIR / "banned.json"
WORKSPACE_PATH = DATA_DIR / "workspaces.json"

ZCODE_BIN = "/usr/local/bin/zcode"
ZCODE_DB = "/root/.zcode/cli/db/db.sqlite"
ZCODE_CONFIG = Path("/root/.zcode/cli/config.json")

SESSION_ID_RE = re.compile(r"^sess_[A-Za-z0-9_\-]{6,80}$")
PROVIDER_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,40}$")
WORKSPACE_NAME_RE = re.compile(r"^[\w\u4e00-\u9fa5 .\-]{1,60}$", re.UNICODE)

VALID_MODES = ("plan", "build", "edit", "yolo")

# 模式的中文名与说明（前端也用这份，保持单一来源）
MODE_META = [
    {"value": "plan",  "label": "规划",   "short": "只读",
     "desc": "只做分析和规划，不改动任何文件。适合先想清楚再动手。"},
    {"value": "build", "label": "构建",   "short": "常规",
     "desc": "日常默认。可读写文件、执行命令，遇到高风险操作会先征求确认。"},
    {"value": "edit",  "label": "编辑",   "short": "仅编辑",
     "desc": "允许直接改文件，但限制执行命令等有副作用的操作。"},
    {"value": "yolo",  "label": "放行",   "short": "全权限",
     "desc": "不再询问，所有操作直接执行。请只在完全信任的任务里使用。"},
]

# app-server 反向请求「运行时偏好」的应答（schema 来自官方 runtime 反查）
RUNTIME_PREFERENCES = {
    "nativeSearchEnhancementsEnabled": False,
    "memoryEnabled": False,
    "askUserQuestionAutoResolutionEnabled": True,
    "modelContextBudgetStrategy": "preflight-v1",
}

# 官方厂商的域名，用于把 provider 归类到「官方配置 / 自定义配置」
OFFICIAL_HOSTS = {"api.z.ai", "open.bigmodel.cn", "zcode.z.ai"}

DEFAULT_LOGIN_KEY = "admin123"

EVENT_BUFFER_MAX = 4000        # 每个会话缓存多少条事件供迟到者回放
EVENT_POLL_INTERVAL = 0.35     # 拉取事件的间隔
APP_SERVER_CALL_TIMEOUT = 90
TURN_IDLE_TIMEOUT = 900        # 单轮最长执行时间
MAX_MESSAGE_CHARS = 20000

DEFAULT_CONFIG = {
    "port": 8081,
    "bind": "0.0.0.0",
    "key_sha256": "",
    "key_plain": "",
    "max_attempts": 5,
    "token_ttl": 43200,
    "allow_cidr": [],
    "trust_proxy": False,
    "default_mode": "build",
    "default_thought_level": "",
    "auto_approve": True,
    "workspace_root": "/root",
}

config = dict(DEFAULT_CONFIG)
ban_store = None
token_store = None
zc = None            # ZCodeServer
feed = None          # SessionFeedManager
runner = None        # TurnManager


def log(msg):
    print("[%s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def _consume_future(fut):
    """把没人 await 的 future 异常取走，否则日志里会刷 'Future exception was never retrieved'。"""
    try:
        if not fut.cancelled():
            fut.exception()
    except Exception:
        pass


def load_config():
    global config
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_PATH.exists():
        try:
            cfg.update(json.loads(CONFIG_PATH.read_text(encoding="utf-8")))
        except Exception as exc:
            log(f"配置读取失败，使用默认值: {exc}")
    config = cfg
    return config


def save_config():
    tmp = CONFIG_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(CONFIG_PATH)
    try:
        os.chmod(CONFIG_PATH, 0o600)
    except Exception:
        pass


def hash_key(key):
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def current_login_key():
    """当前生效的登录密钥明文（没设过就是默认值）。"""
    return config.get("key_plain") or DEFAULT_LOGIN_KEY


def verify_key(key):
    expect = config.get("key_sha256") or hash_key(DEFAULT_LOGIN_KEY)
    return hmac.compare_digest(hash_key(key), str(expect))


# ---------------------------------------------------------------- 封禁与令牌

class BanStore:
    """失败计数与永久封禁。名单落盘，按 mtime 热加载（改完不用重启）。"""

    def __init__(self, path):
        self.path = path
        self.data = {}
        self._mtime = None
        self.load()

    def load(self):
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8")) or {}
            except Exception:
                self.data = {}
            try:
                self._mtime = self.path.stat().st_mtime_ns
            except Exception:
                self._mtime = None
        else:
            self.data = {}
            self._mtime = None

    def reload_if_changed(self):
        if not self.path.exists():
            if self.data:
                self.data = {}
                self._mtime = None
            return
        try:
            mtime = self.path.stat().st_mtime_ns
        except Exception:
            return
        if mtime != self._mtime:
            self.load()
            log(f"封禁名单已被外部修改，重新载入（{len(self.data)} 条）")

    def save(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)
        try:
            self._mtime = self.path.stat().st_mtime_ns
        except Exception:
            self._mtime = None

    def is_banned(self, ip):
        rec = self.data.get(ip)
        return bool(rec and rec.get("banned"))

    def record_fail(self, ip, limit):
        rec = self.data.setdefault(ip, {"fails": 0, "banned": False})
        rec["fails"] = int(rec.get("fails", 0)) + 1
        rec["last_fail_at"] = int(time.time())
        if rec["fails"] >= limit:
            rec["banned"] = True
        self.save()
        return rec

    def reset(self, ip):
        if ip in self.data:
            self.data.pop(ip)
            self.save()


class TokenStore:
    def __init__(self, ttl):
        self.ttl = ttl
        self.tokens = {}

    def issue(self, ip):
        tok = secrets.token_urlsafe(32)
        self.tokens[tok] = {"ip": ip, "exp": time.time() + self.ttl}
        return tok

    def valid(self, tok):
        rec = self.tokens.get(tok)
        if not rec:
            return False
        if rec["exp"] < time.time():
            self.tokens.pop(tok, None)
            return False
        return True

    def revoke(self, tok):
        self.tokens.pop(tok, None)


# ---------------------------------------------------------------- 工作区注册表

def load_workspaces():
    if WORKSPACE_PATH.exists():
        try:
            d = json.loads(WORKSPACE_PATH.read_text(encoding="utf-8"))
            return d.get("workspaces") or []
        except Exception:
            return []
    return []


def save_workspaces(items):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = WORKSPACE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps({"workspaces": items}, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    tmp.replace(WORKSPACE_PATH)


def ensure_workspace_registered(path, purpose="project", name=None):
    """确保某个目录在注册表里；返回登记项。"""
    path = os.path.abspath(path)
    items = load_workspaces()
    for it in items:
        if it.get("path") == path:
            return it
    it = {
        "id": "ws_" + hashlib.sha1(path.encode()).hexdigest()[:12],
        "name": name or os.path.basename(path.rstrip("/")) or path,
        "path": path,
        "purpose": purpose if purpose in ("project", "task") else "project",
        "createdAt": int(time.time() * 1000),
    }
    items.append(it)
    save_workspaces(items)
    return it


# ---------------------------------------------------------------- SQLite（历史与归档）

def db_connect():
    conn = sqlite3.connect("file:%s?mode=ro" % ZCODE_DB, uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def db_write():
    conn = sqlite3.connect(ZCODE_DB, timeout=15)
    conn.row_factory = sqlite3.Row
    return conn


def load_messages(session_id):
    """还原一个会话的完整对话（读 zcode 自己的库）。"""
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, data, sequence, time_created FROM message WHERE session_id = ? ORDER BY sequence",
            (session_id,),
        )
        rows = cur.fetchall()
        out = []
        for r in rows:
            try:
                meta = json.loads(r["data"] or "{}")
            except Exception:
                meta = {}
            cur.execute("SELECT data, sequence FROM part WHERE message_id = ? ORDER BY sequence",
                        (r["id"],))
            parts = []
            for p in cur.fetchall():
                try:
                    parts.append(json.loads(p["data"] or "{}"))
                except Exception:
                    continue
            model = meta.get("modelID")
            if not model and isinstance(meta.get("model"), dict):
                model = meta["model"].get("modelID")
            out.append({
                "id": r["id"],
                "role": meta.get("role", "unknown"),
                "time": r["time_created"],
                "model": model,
                "error": meta.get("error"),
                "parts": parts,
            })
        return out
    finally:
        conn.close()


def session_row(session_id):
    conn = db_connect()
    try:
        r = conn.execute(
            "SELECT id, title, directory, task_type, time_created, time_updated, time_archived "
            "FROM session WHERE id = ?", (session_id,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def archived_sessions():
    """归档的会话：协议 session/list 不返回它们，所以直接查库。"""
    conn = db_connect()
    try:
        rows = conn.execute(
            "SELECT id, title, directory, task_type, time_created, time_updated, time_archived "
            "FROM session WHERE time_archived IS NOT NULL ORDER BY time_archived DESC LIMIT 200"
        ).fetchall()
        out = []
        for r in rows:
            cnt = conn.execute("SELECT COUNT(*) FROM message WHERE session_id=?", (r["id"],)).fetchone()[0]
            out.append({
                "id": r["id"],
                "title": r["title"] or "(无标题)",
                "workspacePath": r["directory"] or "",
                "workspaceKey": r["directory"] or "",
                "created": r["time_created"],
                "updated": r["time_updated"],
                "archivedAt": r["time_archived"],
                "messages": cnt,
                "archived": True,
                "status": "archived",
                "kind": r["task_type"] or "interactive",
            })
        return out
    finally:
        conn.close()


def archive_session(session_id, archived=True):
    conn = db_write()
    try:
        cur = conn.execute("UPDATE session SET time_archived = ? WHERE id = ?",
                           (int(time.time() * 1000) if archived else None, session_id))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def delete_session(session_id):
    """彻底删除一个会话及其消息、部件、所有带 session_id 的关联记录。

    表结构会随 zcode 版本增加（workflow_* / *_usage / session_input ...），
    所以不写死表名，改成扫一遍 schema 找带 session_id 列的表。
    """
    conn = db_write()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM message WHERE session_id = ?", (session_id,))
        mids = [r[0] for r in cur.fetchall()]
        for mid in mids:
            cur.execute("DELETE FROM part WHERE message_id = ?", (mid,))
        cur.execute("DELETE FROM message WHERE session_id = ?", (session_id,))

        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        for tbl in tables:
            if tbl == "session":
                continue
            try:
                cols = [d[1] for d in cur.execute("PRAGMA table_info(%s)" % tbl)]
            except Exception:
                continue
            if "session_id" not in cols:
                continue
            try:
                cur.execute("DELETE FROM %s WHERE session_id = ?" % tbl, (session_id,))
            except Exception:
                pass

        cur.execute("DELETE FROM session WHERE id = ?", (session_id,))
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        conn.close()


def session_title(session_id):
    r = session_row(session_id)
    return (r or {}).get("title") or ""


# ---------------------------------------------------------------- ZCode Protocol 客户端

class ZCodeServer:
    """长期运行的 `zcode app-server` 子进程客户端。

    协议要点：
      * 请求 {id, method, params}；响应 {id, result|error}
      * 服务端也会**反向发请求**（带 id 和 method），必须应答，否则它超时
      * 其余带 method 无 id 的是通知
    """

    def __init__(self):
        self.proc = None
        self.pending = {}
        self.seq = 0
        self.reader_task = None
        self.stderr_task = None
        self.lock = asyncio.Lock()
        self.ready = asyncio.Event()
        self.notification_handlers = []      # fn(method, params)
        self.server_request_log = []
        self.permission_events = []
        self.auto_approve = True
        # 重启必须串行：并发的 stop/start 会把对方刚拉起来的进程再杀掉，
        # 结果就是 self.proc 指向一个已死进程，之后所有调用都报「app-server 未运行」。
        self.restart_lock = asyncio.Lock()
        # 配置文件是「读-改-写」，两个请求交错会丢更新，串行化一下
        self.config_lock = asyncio.Lock()

    async def start(self):
        env = {**os.environ, "HOME": "/root", "TERM": "dumb"}
        self.proc = await asyncio.create_subprocess_exec(
            ZCODE_BIN, "app-server",
            cwd=config.get("workspace_root", "/root"),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
            env=env,
        )
        self.ready.clear()
        self.reader_task = asyncio.create_task(self._reader())
        self.stderr_task = asyncio.create_task(self._drain_stderr())
        # 这里**故意不同步等待握手**：app-server 在我们发第一个请求之前不会说任何话，
        # 而第一个请求本来就要等 MCP 初始化（冷启动十几秒）。
        # 同步等只会让「归档 / 改配置」这类要重启 app-server 的操作白白卡 25 秒。
        # 真正的预热交给 on_startup 里的 warm_options() 在后台做。
        log("zcode app-server 已启动 pid=%s" % self.proc.pid)

    async def _drain_stderr(self):
        try:
            while True:
                line = await self.proc.stderr.readline()
                if not line:
                    break
                txt = line.decode("utf-8", "replace").strip()
                if txt:
                    log("app-server stderr: " + txt[:400])
        except Exception:
            pass

    async def _reader(self):
        try:
            while True:
                line = await self.proc.stdout.readline()
                if not line:
                    break
                raw = line.decode("utf-8", "replace").strip()
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                await self._dispatch(msg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log(f"app-server 读线程异常: {exc}")
        finally:
            for fut in list(self.pending.values()):
                if not fut.done():
                    fut.set_exception(RuntimeError("app-server 已断开"))
                else:
                    _consume_future(fut)
            self.pending.clear()

    async def _dispatch(self, msg):
        mid = msg.get("id")
        method = msg.get("method")

        # 服务端 → 客户端 的请求：必须应答
        if mid is not None and method:
            await self._answer_server_request(mid, method, msg.get("params") or {})
            return

        # 我们发出的请求的响应
        if mid is not None and ("result" in msg or "error" in msg):
            fut = self.pending.pop(mid, None)
            if fut and not fut.done():
                if "error" in msg:
                    fut.set_exception(RuntimeError(
                        "%s" % (msg["error"].get("message") or msg["error"])))
                else:
                    fut.set_result(msg.get("result"))
            return

        # 通知
        if method:
            for h in list(self.notification_handlers):
                try:
                    h(method, msg.get("params") or {})
                except Exception:
                    pass

    async def _answer_server_request(self, mid, method, params):
        """应答服务端的反向请求。这是协议里最容易踩坑的地方。"""
        result = {}
        if method == "session/requestRuntimePreferences":
            result = dict(RUNTIME_PREFERENCES)
        elif method == "interaction/requestPermission":
            allow = bool(config.get("auto_approve", True))
            result = {"decision": "allow" if allow else "deny",
                      "reason": "网页控制台自动放行" if allow else "网页控制台已关闭自动放行"}
            self.permission_events.append({
                "at": int(time.time() * 1000),
                "toolName": params.get("toolName"),
                "riskLevel": params.get("riskLevel"),
                "reason": params.get("reason"),
                "decision": result["decision"],
            })
            del self.permission_events[:-50]
            log(f"权限请求：{params.get('toolName')} risk={params.get('riskLevel')} -> {result['decision']}")
        elif method == "interaction/requestUserInput":
            result = {"action": "decline"}
        elif method in ("interaction/requestOfficialMcpAuthHeaders",
                        "interaction/requestProviderRuntimeHeaders"):
            result = {}
        else:
            self.server_request_log.append({"at": int(time.time() * 1000), "method": method})
            del self.server_request_log[:-50]
            result = {}
        await self._write({"id": mid, "result": result})

    async def _write(self, obj):
        async with self.lock:
            if not self.proc or self.proc.returncode is not None:
                raise RuntimeError("app-server 未运行")
            self.proc.stdin.write(json.dumps(obj, ensure_ascii=False).encode() + b"\n")
            await self.proc.stdin.drain()

    async def call(self, method, params=None, timeout=APP_SERVER_CALL_TIMEOUT):
        self.seq += 1
        rid = "r%d" % self.seq
        fut = asyncio.get_running_loop().create_future()
        fut.add_done_callback(_consume_future)
        self.pending[rid] = fut
        await self._write({"id": rid, "method": method, "params": params or {}})
        try:
            res = await asyncio.wait_for(fut, timeout=timeout)
            self.ready.set()
            return res
        except asyncio.TimeoutError:
            self.pending.pop(rid, None)
            raise RuntimeError(f"调用 {method} 超时（{timeout}s）")

    async def stop(self):
        for t in (self.reader_task, self.stderr_task):
            if t and not t.done():
                t.cancel()
        if self.proc and self.proc.returncode is None:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
        self.proc = None

    async def restart(self, reason=""):
        """重启 app-server。改过配置或数据库（归档/删除）之后需要。"""
        async with self.restart_lock:
            log(f"重启 app-server{('：' + reason) if reason else ''}")
            await self.stop()
            await asyncio.sleep(0.4)
            await self.start()
            # 重启后旧订阅与事件游标全部失效，必须整体重置，否则前端会收到错乱的增量
            if feed:
                await feed.reset_all(reason)
            _options_cache.clear()
            # 新进程的 provider registry 与 MCP 都要重建，后台先做好，
            # 别让用户点开界面干等，也别让老会话因为「模型不可用」而发不出消息
            asyncio.create_task(bootstrap_catalog(reason))


# ---------------------------------------------------------------- 会话事件流

class SessionFeed:
    """单个会话的事件流：建立订阅 + 按 seq 拉增量 + 广播给前端。"""

    def __init__(self, session_id):
        self.session_id = session_id
        self.cursor = 0
        self.subscribers = set()
        self.buffer = []            # 翻译后的事件，供迟到者回放
        self.task = None
        self.turn_active = False
        self.turn_done = False      # 本轮是否已收到收尾事件（turn.completed / failed）
        self.subscribed = False
        self.last_activity = time.time()
        self.usage = None
        self.error = None
        self.lock = asyncio.Lock()

    def add_subscriber(self, ws):
        self.subscribers.add(ws)

    def remove_subscriber(self, ws):
        self.subscribers.discard(ws)

    async def broadcast(self, payload):
        self.buffer.append(payload)
        if len(self.buffer) > EVENT_BUFFER_MAX:
            del self.buffer[:len(self.buffer) - EVENT_BUFFER_MAX]
        dead = []
        for ws in list(self.subscribers):
            try:
                if ws.closed:
                    dead.append(ws)
                    continue
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.subscribers.discard(ws)


class SessionFeedManager:
    def __init__(self):
        self.feeds = {}
        self.tasks = {}

    def get(self, session_id):
        return self.feeds.get(session_id)

    async def ensure_subscription(self, session_id):
        """建立（或复用）订阅，返回 feed。

        订阅必须成功：`eventSeq` 就是后续拉增量事件的起始游标。
        拿不到游标的话 afterSeq 只能从 0 开始，会把整段历史当事件重放一遍，
        前端就会和 SQLite 历史渲染叠在一起出现重复内容 —— 所以这里要重试。
        """
        f = self.feeds.get(session_id)
        if f:
            return f
        f = SessionFeed(session_id)
        self.feeds[session_id] = f
        last_err = None
        for attempt in range(1, 4):
            try:
                res = await zc.call("session/subscribe",
                                    {"sessionId": session_id,
                                     "deliveryKind": "web-remote-replayable"}, timeout=30)
                f.cursor = int((res or {}).get("eventSeq") or 0)
                f.subscribed = True
                return f
            except Exception as exc:
                last_err = exc
                log(f"订阅会话失败（第 {attempt} 次）{session_id}: {exc}")
                if attempt < 3:
                    await asyncio.sleep(0.5 * attempt)
        await f.broadcast({"type": "error", "sessionId": session_id,
                           "message": f"无法订阅会话事件流：{last_err}"})
        return f

    async def start_pump(self, session_id):
        """为一个会话启动事件拉取任务（如果还没在跑）。"""
        f = await self.ensure_subscription(session_id)
        f.turn_active = True
        f.turn_done = False
        f.error = None
        f.usage = None
        f.last_activity = time.time()
        # 只保留本轮的事件：迟到者重连时前端已经渲染过历史消息，
        # 要是把上一轮的缓冲一起回放就会整段重复。
        f.buffer.clear()
        task = self.tasks.get(session_id)
        if task and not task.done():
            return f
        self.tasks[session_id] = asyncio.create_task(self._pump(session_id))
        return f

    async def reset_all(self, reason=""):
        """app-server 重启后所有订阅与游标都失效，整体清空重建。"""
        for sid, f in list(self.feeds.items()):
            f.turn_active = False
            try:
                await f.broadcast({"type": "error", "sessionId": sid,
                                   "message": "服务配置已更新，连接已重置，请重新发送消息"})
                await f.broadcast({"type": "done", "sessionId": sid, "reset": True})
            except Exception:
                pass
        for t in list(self.tasks.values()):
            if t and not t.done():
                t.cancel()
        self.tasks.clear()
        self.feeds.clear()
        if runner:
            runner.running.clear()
            # 新进程不知道这些会话的当前模式/模型/思考等级，下次发送时重新同步
            runner.desired.clear()
        if reason:
            log(f"事件流已全部重置（{reason}）")

    async def _pump(self, session_id):
        f = self.feeds.get(session_id)
        if not f:
            return
        log(f"开始拉取事件: {session_id}")
        try:
            while True:
                if not f.turn_active:
                    break
                if time.time() - f.last_activity > TURN_IDLE_TIMEOUT:
                    await f.broadcast({"type": "error", "sessionId": session_id,
                                       "message": "执行超时，已停止接收事件"})
                    break
                try:
                    res = await zc.call("session/events", {
                        "sessionId": session_id, "afterSeq": f.cursor, "limit": 200}, timeout=20)
                except Exception as exc:
                    log(f"拉取事件失败 {session_id}: {exc}")
                    await asyncio.sleep(1.5)
                    continue

                events = (res or {}).get("events") or []
                if events:
                    f.last_activity = time.time()
                for e in events:
                    seq = e.get("seq")
                    if isinstance(seq, int):
                        f.cursor = max(f.cursor, seq + 1)
                    await self._handle_event(f, e)

                if f.turn_done:
                    # 先释放「执行中」再广播 done，否则前端收到 done 后立刻发下一条会被拒
                    if runner:
                        runner.finish(session_id)
                    await f.broadcast({"type": "done", "sessionId": session_id,
                                       "usage": f.usage, "title": session_title(session_id)})
                    break
                await asyncio.sleep(EVENT_POLL_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log(f"事件拉取任务异常 {session_id}: {exc}")
        finally:
            f.turn_active = False
            # 关键：把会话从「执行中」放出来，否则后续消息会被判定为并发而拒绝
            if runner:
                runner.finish(session_id)
            log(f"事件拉取结束: {session_id}")

    async def _handle_event(self, f, e):
        etype = e.get("type") or ""
        pl = e.get("payload") or {}
        sid = f.session_id

        if etype == "turn.started":
            await f.broadcast({"type": "started", "sessionId": sid,
                               "messageId": pl.get("messageId"),
                               "turnNumber": pl.get("turnNumber")})
            return

        if etype == "session.titleUpdated":
            await f.broadcast({"type": "title", "sessionId": sid,
                               "title": pl.get("title") or ""})
            return

        if etype == "model.streaming":
            kind = pl.get("kind")
            mid = pl.get("assistantMessageId")
            if kind == "text_delta":
                await f.broadcast({"type": "delta", "sessionId": sid,
                                   "messageId": mid, "text": pl.get("delta") or ""})
            elif kind in ("reasoning_delta", "thought_delta"):
                await f.broadcast({"type": "reasoning", "sessionId": sid,
                                   "messageId": mid, "text": pl.get("delta") or ""})
            elif kind == "tool_call":
                await f.broadcast({"type": "tool", "sessionId": sid, "messageId": mid,
                                   "toolCallId": pl.get("toolCallId"),
                                   "toolName": pl.get("toolName"),
                                   "state": "call", "input": pl.get("input")})
            elif kind == "tool_input_delta":
                await f.broadcast({"type": "tool", "sessionId": sid, "messageId": mid,
                                   "toolCallId": pl.get("toolCallId"),
                                   "state": "input_delta", "delta": pl.get("delta") or ""})
            elif kind == "tool_input_end":
                await f.broadcast({"type": "tool", "sessionId": sid, "messageId": mid,
                                   "toolCallId": pl.get("toolCallId"), "state": "input_end"})
            else:
                await f.broadcast({"type": "stream", "sessionId": sid, "messageId": mid,
                                   "kind": kind, "payload": pl})
            return

        if etype == "tool.updated":
            await f.broadcast({"type": "tool", "sessionId": sid,
                               "messageId": pl.get("assistantMessageId"),
                               "toolCallId": pl.get("toolCallId"),
                               "toolName": pl.get("toolName"),
                               "state": pl.get("kind") or "updated",
                               "payload": pl})
            return

        if etype == "streamRecovery.updated":
            await f.broadcast({"type": "tool", "sessionId": sid,
                               "messageId": pl.get("assistantMessageId"),
                               "toolCallId": pl.get("toolCallId"),
                               "toolName": pl.get("toolName"),
                               "state": "result",
                               "payload": pl})
            return

        if etype == "turn.completed":
            f.usage = pl.get("usage") or {}
            f.turn_done = True
            await f.broadcast({"type": "turnEnd", "sessionId": sid,
                               "response": pl.get("response"),
                               "usage": f.usage,
                               "duration": pl.get("duration"),
                               "toolCallCount": pl.get("toolCallCount"),
                               "tokenCount": pl.get("tokenCount")})
            return

        if etype == "turn.failed" or etype == "session.error":
            f.error = pl
            f.turn_done = True
            await f.broadcast({"type": "error", "sessionId": sid,
                               "message": pl.get("message") or pl.get("error") or "执行失败"})
            return

        if etype == "session.updated":
            # 有些回复（尤其是极短回复）上游不吐 SSE 分片，这时不会有 text_delta，
            # 整段文本只在这条 session.updated 的 content 里出现一次。
            # 把它作为「权威文本」推给前端，保证气泡永远不会是空的。
            content = pl.get("content")
            if isinstance(content, str) and content:
                await f.broadcast({"type": "final", "sessionId": sid,
                                   "messageId": pl.get("messageId"),
                                   "text": content})
            else:
                await f.broadcast({"type": "meta", "sessionId": sid,
                                   "event": etype, "payload": pl})
            return

        if etype == "state.updated":
            await f.broadcast({"type": "meta", "sessionId": sid,
                               "event": etype, "payload": pl})


# ---------------------------------------------------------------- 对话执行

class TurnManager:
    """负责发消息、取消、以及确保会话处于我们想要的模式/模型/思考等级。"""

    def __init__(self):
        self.lock = asyncio.Lock()
        self.running = {}       # session_id -> {"startedAt": ms}
        self.desired = {}       # session_id -> {"mode","model","thoughtLevel"}

    def is_running(self, session_id):
        return session_id in self.running

    async def apply_settings(self, session_id, mode=None, model=None, thought_level=None):
        """把模式 / 模型 / 思考等级同步到会话上（只在有变化时调用）。"""
        want = self.desired.get(session_id) or {}
        changed = False
        if mode and want.get("mode") != mode:
            await zc.call("session/setMode", {"sessionId": session_id, "mode": mode}, timeout=30)
            want["mode"] = mode
            changed = True
        if model and want.get("model") != model:
            await zc.call("session/setModel", {"sessionId": session_id, "model": model}, timeout=30)
            want["model"] = model
            changed = True
        if thought_level and want.get("thoughtLevel") != thought_level:
            await zc.call("session/setThoughtLevel",
                          {"sessionId": session_id, "thoughtLevel": thought_level}, timeout=30)
            want["thoughtLevel"] = thought_level
            changed = True
        self.desired[session_id] = want
        return changed

    async def resume(self, session_id, workspace=None):
        params = {"sessionId": session_id}
        if workspace:
            params["workspace"] = workspace
        return await zc.call("session/resume", params, timeout=60)

    async def create(self, workspace, mode=None, model=None, thought_level=None):
        params = {"workspace": workspace}
        if mode:
            params["mode"] = mode
        if model:
            params["model"] = model
        if thought_level:
            params["thoughtLevel"] = thought_level
        res = await zc.call("session/create", params, timeout=60)
        sid = ((res or {}).get("session") or {}).get("sessionId")
        if not sid:
            # 拿不到会话 id 时必须立刻报错，否则后面所有调用都会带着 None 往下传
            raise RuntimeError("zcode 没有返回会话 ID（app-server 可能未就绪，请稍后重试）")
        self.desired[sid] = {"mode": mode, "model": model, "thoughtLevel": thought_level}
        return res, sid

    async def ensure_active(self, session_id):
        """确保会话在 app-server 里是激活的。

        新建的会话本来就在内存里；但从侧栏点开的**旧会话**不一定还激活着，
        `session/send` 会报 not active。所以先 resume 一次把它拉起来。
        """
        if session_id in self.desired:
            return
        row = session_row(session_id) or {}
        ws_path = row.get("directory") or config.get("workspace_root", "/root")
        try:
            await self.resume(session_id,
                              {"workspaceKey": ws_path, "workspacePath": ws_path})
            log(f"已激活旧会话: {session_id}")
        except Exception as exc:
            log(f"激活旧会话失败 {session_id}: {exc}")
        self.desired.setdefault(session_id, {})

    @staticmethod
    def _stale_model_error(exc):
        msg = str(exc)
        return "模型" in msg and "不可用" in msg

    async def send(self, session_id, content, mode=None, model=None, thought_level=None):
        async with self.lock:
            if session_id in self.running:
                raise RuntimeError("这个会话正在执行中，请稍候")
            await self.ensure_active(session_id)
            await self.apply_settings(session_id, mode, model, thought_level)
            await feed.start_pump(session_id)
            self.running[session_id] = {"startedAt": int(time.time() * 1000)}
            payload = {"sessionId": session_id, "content": content}
            try:
                res = await zc.call("session/send", payload, timeout=60)
            except Exception as exc:
                res = None
                if self._stale_model_error(exc):
                    res = await self._retry_with_runtime_model(session_id, payload)
                if res is None:
                    self.running.pop(session_id, None)
                    f = feed.get(session_id)
                    if f:
                        f.turn_active = False      # 别让拉取任务空转到超时
                    raise
        return res

    async def _retry_with_runtime_model(self, session_id, payload):
        """会话历史里记的模型已经不可用了。

        zcode 在这种情况下会在 session/send 开头直接抛 restoreWarning，
        但**带 runtimeModel 的请求**会先走 applyRuntimeModelSelectionToRecord，
        顺手把 warning 清掉。所以这里补一个 runtimeModel 再发一次。
        """
        want = self.desired.get(session_id) or {}
        ref = want.get("model") if isinstance(want.get("model"), dict) else None
        row = session_row(session_id) or {}
        ws_path = row.get("directory") or config.get("workspace_root", "/root")
        rt = None
        try:
            rt = build_runtime_model(ws_path,
                                     (ref or {}).get("providerId"),
                                     (ref or {}).get("modelId"))
        except Exception as exc:
            log(f"构造 runtimeModel 失败: {exc}")
        if not rt:
            log(f"会话 {session_id} 模型不可用，且没有可回落的模型")
            return None
        log("会话 %s 历史模型不可用，改用 %s/%s 重试（带 runtimeModel）"
            % (session_id, rt["model"]["providerId"], rt["model"]["modelId"]))
        try:
            payload = dict(payload)
            payload["runtimeModel"] = rt
            res = await zc.call("session/send", payload, timeout=60)
            want["model"] = dict(rt["model"])
            self.desired[session_id] = want
            return res
        except Exception as exc:
            log(f"带 runtimeModel 重试仍然失败 {session_id}: {exc}")
            return None

    async def cancel(self, session_id):
        if session_id not in self.running:
            return False
        try:
            await zc.call("session/stop", {"sessionId": session_id}, timeout=30)
        except Exception as exc:
            log(f"停止会话失败 {session_id}: {exc}")
        f = feed.get(session_id)
        if f:
            f.turn_active = False
            f.turn_done = True
            await f.broadcast({"type": "cancelled", "sessionId": session_id})
            await f.broadcast({"type": "done", "sessionId": session_id, "cancelled": True})
        self.running.pop(session_id, None)
        return True

    def finish(self, session_id):
        self.running.pop(session_id, None)


# ---------------------------------------------------------------- 模型与厂商配置

def read_zcode_config():
    if ZCODE_CONFIG.exists():
        try:
            return json.loads(ZCODE_CONFIG.read_text(encoding="utf-8"))
        except Exception as exc:
            log(f"zcode 配置解析失败: {exc}")
    return {}


def write_zcode_config(cfg):
    backup = ZCODE_CONFIG.with_suffix(".json.webconsole-bak")
    try:
        if ZCODE_CONFIG.exists() and not backup.exists():
            shutil.copy2(ZCODE_CONFIG, backup)
    except Exception:
        pass
    tmp = ZCODE_CONFIG.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(ZCODE_CONFIG)
    try:
        os.chmod(ZCODE_CONFIG, 0o600)
    except Exception:
        pass


def classify_provider(pid, pconf):
    """官方 / 自定义：看 baseURL 域名。"""
    base = ((pconf.get("options") or {}).get("baseURL") or "")
    host = ""
    try:
        host = base.split("//", 1)[1].split("/", 1)[0].split(":")[0].lower()
    except Exception:
        host = ""
    is_official = host in OFFICIAL_HOSTS
    return "official" if is_official else "custom"


def mask_key(k):
    if not k:
        return ""
    if len(k) <= 8:
        return "*" * len(k)
    return k[:4] + "*" * max(4, len(k) - 8) + k[-4:]


def provider_list(with_secrets=False):
    cfg = read_zcode_config()
    providers = cfg.get("provider") or {}
    main = (cfg.get("model") or {}).get("main") or ""
    out = []
    for pid, p in providers.items():
        models = []
        for mid, m in (p.get("models") or {}).items():
            lim = m.get("limit") or {}
            models.append({
                "modelId": mid,
                "name": m.get("name") or mid,
                "context": lim.get("context"),
                "output": lim.get("output"),
                "isMain": main == "%s/%s" % (pid, mid),
                "modalities": m.get("modalities"),
            })
        opts = p.get("options") or {}
        api_key = opts.get("apiKey") or ""
        out.append({
            "providerId": pid,
            "name": p.get("name") or pid,
            "kind": p.get("kind") or "openai-compatible",
            "baseURL": opts.get("baseURL") or "",
            "apiKeyRequired": bool(opts.get("apiKeyRequired", True)),
            "apiKeyMasked": mask_key(api_key),
            "apiKey": api_key if with_secrets else "",
            "hasApiKey": bool(api_key),
            "headers": p.get("headers") or {},
            "models": models,
            "scope": classify_provider(pid, p),
            "source": "config",
        })
    return out


def _merge_model(models, m):
    """把前端传的一个模型条目并进 provider 的 models 字典。"""
    if not isinstance(m, dict):
        return
    mid = str(m.get("modelId") or "").strip()
    if not mid:
        return
    item = dict(models.get(mid) or {})
    item["name"] = str(m.get("name") or item.get("name") or mid)
    lim = dict(item.get("limit") or {})
    for key in ("context", "output"):
        if m.get(key) not in (None, ""):
            try:
                lim[key] = int(m[key])
            except Exception:
                pass
    if lim:
        item["limit"] = lim
    if m.get("supportsImages") is not None:
        item["modalities"] = {
            "input": ["text", "image"] if m["supportsImages"] else ["text"],
            "output": ["text"],
        }
    models[mid] = item


def build_provider_entry(data, existing=None):
    """从请求体构造 provider 配置项（支持一并带上模型列表）。"""
    existing = existing or {}
    kind = data.get("kind") or existing.get("kind") or "openai-compatible"
    if kind not in ("openai-compatible", "anthropic"):
        kind = "openai-compatible"
    opts = dict(existing.get("options") or {})
    if "baseURL" in data:
        opts["baseURL"] = str(data.get("baseURL") or "").strip()
    if data.get("apiKey"):
        opts["apiKey"] = str(data["apiKey"]).strip()
    if data.get("clearApiKey"):
        opts.pop("apiKey", None)
    opts.setdefault("apiKeyRequired", True)

    entry = dict(existing)
    entry["kind"] = kind
    if data.get("name"):
        entry["name"] = str(data["name"]).strip()
    entry["options"] = opts
    entry.setdefault("headers", {})

    models = dict(existing.get("models") or {})
    for m in (data.get("models") or []):
        _merge_model(models, m)
    for mid in (data.get("removeModels") or []):
        models.pop(str(mid), None)
    entry["models"] = models
    return entry


# ---------------------------------------------------------------- 请求辅助

def client_ip(request):
    if config.get("trust_proxy"):
        fwd = request.headers.get("X-Forwarded-For", "")
        if fwd:
            return fwd.split(",")[0].strip()
    return request.remote or "-"


def ip_allowed(ip):
    rules = config.get("allow_cidr") or []
    if not rules:
        return True
    import ipaddress
    try:
        addr = ipaddress.ip_address(ip)
    except Exception:
        return False
    for rule in rules:
        try:
            if addr in ipaddress.ip_network(rule, strict=False):
                return True
        except Exception:
            continue
    return False


def get_token(request):
    tok = request.headers.get("X-Auth-Token")
    if tok:
        return tok
    tok = request.cookies.get("zwc_token")
    if tok:
        return tok
    return request.query.get("token")


def is_authed(request):
    tok = get_token(request)
    return bool(tok and token_store.valid(tok))


def json_error(message, status=400, **extra):
    payload = {"ok": False, "error": message}
    payload.update(extra)
    return web.json_response(payload, status=status)


def guard(request):
    ip = client_ip(request)
    ban_store.reload_if_changed()
    if not ip_allowed(ip):
        return json_error("来源地址不在允许范围内", 403, banned=True)
    if ban_store.is_banned(ip):
        return json_error("此 IP 已被永久封禁", 403, banned=True)
    if not is_authed(request):
        return json_error("未登录", 401)
    return None


async def body_json(request):
    try:
        return await request.json()
    except Exception:
        return {}


# ---------------------------------------------------------------- 路由：基础

async def handle_index(request):
    return web.FileResponse(STATIC_DIR / "index.html")


async def handle_health(request):
    ban_store.reload_if_changed()
    return web.json_response({
        "ok": True,
        "service": "zcode-chat",
        "appServer": bool(zc and zc.proc and zc.proc.returncode is None),
        "running": len(runner.running) if runner else 0,
        "banned": sum(1 for v in ban_store.data.values() if v.get("banned")),
        "uptime": int(time.time() - START_TIME),
    })


async def handle_login(request):
    ip = client_ip(request)
    ban_store.reload_if_changed()

    if not ip_allowed(ip):
        return json_error("来源地址不在允许范围内", 403, banned=True)
    if ban_store.is_banned(ip):
        return json_error("此 IP 已被永久封禁", 403, banned=True)

    body = await body_json(request)
    key = str(body.get("key", ""))

    if verify_key(key):
        ban_store.reset(ip)
        token = token_store.issue(ip)
        log(f"登录成功: {ip}")
        resp = web.json_response({"ok": True, "ttl": token_store.ttl, "token": token})
        resp.set_cookie("zwc_token", token, max_age=token_store.ttl,
                        httponly=True, samesite="Lax")
        return resp

    limit = int(config.get("max_attempts", 5))
    rec = ban_store.record_fail(ip, limit)
    remaining = max(0, limit - int(rec.get("fails", 0)))
    if rec.get("banned"):
        log(f"IP 已封禁: {ip}（累计失败 {rec['fails']} 次）")
        return json_error("尝试次数已用尽，此 IP 已被永久封禁", 403, banned=True, remaining=0)
    log(f"密钥错误: {ip}（第 {rec['fails']} 次，剩余 {remaining} 次）")
    return json_error("密钥错误", 401, remaining=remaining)


async def handle_logout(request):
    tok = get_token(request)
    if tok:
        token_store.revoke(tok)
    resp = web.json_response({"ok": True})
    resp.del_cookie("zwc_token")
    return resp


async def handle_me(request):
    ip = client_ip(request)
    ban_store.reload_if_changed()
    if ban_store.is_banned(ip):
        return json_error("此 IP 已被永久封禁", 403, banned=True)
    if not is_authed(request):
        return json_error("未登录", 401)
    return web.json_response({
        "ok": True,
        "ip": ip,
        "defaultMode": config.get("default_mode", "build"),
        "defaultThoughtLevel": config.get("default_thought_level", ""),
        "modes": MODE_META,
        "appServer": bool(zc and zc.proc and zc.proc.returncode is None),
    })


# ---------------------------------------------------------------- 路由：元信息（模型/思考等级）

# workspace/readState 首次调用要等 MCP 初始化，实测可达 60~90 秒；
# 之后的调用很快。所以缓存住，并在服务启动时后台预热一次。
OPTIONS_TTL = 300
_options_cache = {}      # workspace_path -> {"at": ts, "data": payload}
_options_lock = asyncio.Lock()


async def build_options_payload(ws_path, timeout=150):
    state = await zc.call("workspace/readState",
                          {"workspace": {"workspaceKey": ws_path, "workspacePath": ws_path}},
                          timeout=timeout)
    settings = (state or {}).get("settings") or {}
    catalog = (state or {}).get("modelCatalog") or {}

    def norm_model(m):
        ref = m.get("ref") or {}
        return {
            "modelId": ref.get("modelId") or m.get("modelId"),
            "providerId": ref.get("providerId") or m.get("providerId"),
            "label": m.get("label") or ref.get("modelId"),
            "providerLabel": m.get("providerLabel"),
            "context": m.get("contextWindow"),
            "output": m.get("maxOutputTokens"),
            "supportsImages": bool(m.get("supportsImages")),
            "reasoning": m.get("reasoning") or {},
        }

    models = [norm_model(m) for m in ((settings.get("model") or {}).get("available") or [])]
    if not models:
        models = [norm_model(m) for m in (catalog.get("available") or [])]

    tl = settings.get("thoughtLevel") or {}
    cur = (settings.get("model") or {}).get("current") or {}
    return {
        "ok": True,
        "workspace": ws_path,
        "models": models,
        "currentModel": {"providerId": cur.get("providerId"), "modelId": cur.get("modelId")},
        "thoughtLevel": {
            "enabled": bool(tl.get("enabled")),
            "current": tl.get("current"),
            "defaultLevel": tl.get("defaultLevel"),
            "available": tl.get("available") or [],
        },
        "mode": (settings.get("mode") or {}).get("current"),
        "modes": MODE_META,
    }


async def get_options(ws_path, force=False):
    now = time.time()
    hit = _options_cache.get(ws_path)
    if hit and not force and now - hit["at"] < OPTIONS_TTL:
        return hit["data"]
    async with _options_lock:
        hit = _options_cache.get(ws_path)
        if hit and not force and time.time() - hit["at"] < OPTIONS_TTL:
            return hit["data"]
        data = await build_options_payload(ws_path)
        _options_cache[ws_path] = {"at": time.time(), "data": data}
        return data


def build_registry():
    """把 ~/.zcode/cli/config.json 转成 app-server 要的 provider registry。

    这是官方客户端的做法：**客户端才是 provider 配置的来源**，通过
    `workspace/updateProviderRegistry` 把它推给 app-server。推上去之后 app-server
    才有完整的模型目录，也才会自动把「已失效的会话模型」换成可用的那个。
    不推的话，老会话一 resume 就被判定成「历史任务使用的模型已不可用」，直接发不出消息。
    """
    cfg = read_zcode_config()
    providers = []
    for pid, p in (cfg.get("provider") or {}).items():
        opts = p.get("options") or {}
        models = []
        for mid, m in (p.get("models") or {}).items():
            lim = m.get("limit") or {}
            mod = m.get("modalities") or {}
            item = {"modelId": mid,
                    "label": m.get("name") or mid,
                    "supportsTools": True}
            try:
                if lim.get("context"):
                    item["contextWindow"] = int(lim["context"])
                if lim.get("output"):
                    item["maxOutputTokens"] = int(lim["output"])
            except Exception:
                pass
            if mod.get("input"):
                item["supportsImages"] = "image" in mod["input"]
            models.append(item)
        if not models:
            continue
        entry = {
            "providerId": pid,
            "kind": p.get("kind") or "openai-compatible",
            "label": p.get("name") or pid,
            "source": "workspace",
            "apiKeyRequired": bool(opts.get("apiKeyRequired", True)),
            "models": models,
        }
        if opts.get("baseURL"):
            entry["baseURL"] = opts["baseURL"]
        if opts.get("apiKey"):
            entry["apiKey"] = {"source": "inline", "value": opts["apiKey"]}
        if p.get("headers"):
            entry["headers"] = p["headers"]
        providers.append(entry)
    blob = json.dumps(providers, sort_keys=True, ensure_ascii=False)
    return {
        "revision": hashlib.sha1(blob.encode("utf-8")).hexdigest()[:16],
        "generatedAt": int(time.time() * 1000),
        "providers": providers,
    }


def all_workspace_paths():
    root = config.get("workspace_root", "/root")
    paths = [root]
    for it in load_workspaces():
        p = it.get("path")
        if p and p not in paths:
            paths.append(p)
    return paths


async def push_registry(reason="", workspace_paths=None):
    """把 provider registry 推给 app-server（每个工作区一份）。"""
    reg = build_registry()
    if not reg["providers"]:
        log("没有可推送的 provider（配置为空），跳过 registry 同步")
        return False
    paths = workspace_paths or all_workspace_paths()
    ok = False
    for p in paths:
        try:
            await zc.call("workspace/updateProviderRegistry", {
                "workspace": {"workspaceKey": p, "workspacePath": p},
                "registry": reg,
                "includeWorkspaceState": False,
            }, timeout=60)
            ok = True
        except Exception as exc:
            log(f"推送 provider registry 失败 {p}: {exc}")
    if ok:
        log("provider registry 已同步%s（%d 个厂商，%d 个工作区）"
            % (("：" + reason) if reason else "", len(reg["providers"]), len(paths)))
    return ok


def build_runtime_model(ws_path, provider_id=None, model_id=None):
    """构造 session/send 的 runtimeModel —— 传它就能清掉 restoreWarning。"""
    reg = build_registry()
    prov = None
    if provider_id:
        prov = next((p for p in reg["providers"] if p["providerId"] == provider_id), None)
    if prov is None or not model_id:
        main = (read_zcode_config().get("model") or {}).get("main") or ""
        if "/" not in main:
            return None
        pid, mid = main.split("/", 1)
        prov = next((p for p in reg["providers"] if p["providerId"] == pid), None)
        provider_id, model_id = pid, mid
    if prov is None or not model_id:
        return None
    return {
        "revision": reg["revision"],
        "generatedAt": reg["generatedAt"],
        "model": {"providerId": provider_id, "modelId": model_id},
        "provider": prov,
    }


async def bootstrap_catalog(reason=""):
    """推 registry + 预热模型目录。app-server 每次重启都要重来一遍。"""
    try:
        await push_registry(reason)
    except Exception as exc:
        log(f"registry 同步异常: {exc}")
    try:
        await get_options(config.get("workspace_root", "/root"), force=True)
    except Exception as exc:
        log(f"模型目录预热失败（首次访问时会重试）: {exc}")


async def warm_options():
    """启动后在后台把模型目录预热好，免得用户第一次打开界面等一分多钟。"""
    try:
        ws_path = config.get("workspace_root", "/root")
        await get_options(ws_path, force=True)
        log("模型目录预热完成")
    except Exception as exc:
        log(f"模型目录预热失败（首次访问时会重试）: {exc}")


async def handle_options(request):
    blocked = guard(request)
    if blocked:
        return blocked
    ws_path = request.query.get("workspace") or config.get("workspace_root", "/root")
    force = request.query.get("refresh") == "1"
    try:
        data = await get_options(ws_path, force=force)
    except Exception as exc:
        # 预热还没成功时给前端一个能看的兜底，不要整页卡住
        return json_error(f"读取模型目录失败: {exc}", 502)
    return web.json_response(data)


# ---------------------------------------------------------------- 路由：工作区

async def handle_workspaces(request):
    blocked = guard(request)
    if blocked:
        return blocked
    items = load_workspaces()
    # 补充每个工作区的会话数（从协议 session/list 统计）
    counts = {}
    try:
        res = await zc.call("session/list", {}, timeout=40)
        for s in ((res or {}).get("sessions") or []):
            p = ((s.get("workspace") or {}).get("workspacePath")) or ""
            counts[p] = counts.get(p, 0) + 1
    except Exception as exc:
        log(f"统计工作区会话数失败: {exc}")

    root = config.get("workspace_root", "/root")
    out = []
    for it in items:
        out.append({**it, "sessions": counts.get(it["path"], 0)})
    # 把有会话但没登记过的目录也带出来（比如别人手工建的会话）
    known = {it["path"] for it in items}
    for p, n in counts.items():
        if p and p not in known:
            out.append({
                "id": "ws_" + hashlib.sha1(p.encode()).hexdigest()[:12],
                "name": os.path.basename(p.rstrip("/")) or p,
                "path": p,
                "purpose": "project",
                "createdAt": None,
                "sessions": n,
                "unregistered": True,
            })
    out.sort(key=lambda x: (-(x.get("sessions") or 0), x.get("name") or ""))
    return web.json_response({"ok": True, "workspaces": out, "root": root})


async def handle_workspace_create(request):
    blocked = guard(request)
    if blocked:
        return blocked
    data = await body_json(request)
    name = str(data.get("name") or "").strip()
    purpose = str(data.get("purpose") or "project").strip()
    raw_path = str(data.get("path") or "").strip()
    create_dir = bool(data.get("createDir", True))

    if purpose not in ("project", "task"):
        return json_error("用途只能是 project 或 task")
    if not name or not WORKSPACE_NAME_RE.match(name):
        return json_error("名称不合法（1-60 个字符，可用中英文、数字、空格、点、横线、下划线）")

    root = config.get("workspace_root", "/root")
    if raw_path:
        path = os.path.abspath(os.path.join(root, raw_path) if not raw_path.startswith("/") else raw_path)
    else:
        slug = re.sub(r"[^\w\-]+", "-", name, flags=re.UNICODE).strip("-").lower() or "workspace"
        path = os.path.abspath(os.path.join(root, slug))

    # 只允许在工作区根目录下，避免越权访问系统目录
    if not (path == root or path.startswith(root.rstrip("/") + "/")):
        return json_error(f"路径必须位于 {root} 之下")

    if os.path.exists(path) and not os.path.isdir(path):
        return json_error("该路径已存在且不是目录")
    if create_dir and not os.path.exists(path):
        try:
            os.makedirs(path, exist_ok=True)
        except Exception as exc:
            return json_error(f"创建目录失败: {exc}")

    it = ensure_workspace_registered(path, purpose, name)
    log(f"新建工作区: {name} ({purpose}) -> {path}")
    return web.json_response({"ok": True, "workspace": it})


async def handle_workspace_delete(request):
    blocked = guard(request)
    if blocked:
        return blocked
    wid = request.match_info["wid"]
    items = load_workspaces()
    keep = [x for x in items if x.get("id") != wid]
    if len(keep) == len(items):
        return json_error("工作区不存在", 404)
    save_workspaces(keep)
    return web.json_response({"ok": True})


# ---------------------------------------------------------------- 路由：会话

async def handle_sessions(request):
    blocked = guard(request)
    if blocked:
        return blocked
    want_archived = request.query.get("archived") == "1"
    limit = int(request.query.get("limit") or 300)

    if want_archived:
        return web.json_response({"ok": True, "sessions": archived_sessions()})

    try:
        res = await zc.call("session/list", {}, timeout=40)
    except Exception as exc:
        return json_error(f"读取会话列表失败: {exc}", 502)

    out = []
    for s in ((res or {}).get("sessions") or []):
        sid = s.get("sessionId")
        ws = s.get("workspace") or {}
        running = runner.is_running(sid) if runner else False
        out.append({
            "id": sid,
            "title": s.get("title") or "(无标题)",
            "workspacePath": ws.get("workspacePath") or "",
            "workspaceKey": ws.get("workspaceKey") or "",
            "created": s.get("createdAt"),
            "updated": s.get("updatedAt"),
            "status": "running" if running else (s.get("status") or "idle"),
            "mode": s.get("mode"),
            "model": s.get("model"),
            "kind": s.get("sessionKind") or "interactive",
            "archived": False,
        })
    out.sort(key=lambda x: -(x.get("updated") or 0))
    return web.json_response({"ok": True, "sessions": out[:limit]})


async def handle_session_create(request):
    blocked = guard(request)
    if blocked:
        return blocked
    data = await body_json(request)
    ws_path = str(data.get("workspacePath") or "").strip()
    purpose = str(data.get("purpose") or "project").strip()
    mode = str(data.get("mode") or config.get("default_mode", "build"))
    if mode not in VALID_MODES:
        mode = config.get("default_mode", "build")

    root = config.get("workspace_root", "/root")
    if not ws_path:
        ws_path = root
    ws_path = os.path.abspath(ws_path)
    if not (ws_path == root or ws_path.startswith(root.rstrip("/") + "/")):
        return json_error(f"工作区必须位于 {root} 之下")

    model = data.get("model") or None
    if isinstance(model, dict):
        model = {"providerId": model.get("providerId"), "modelId": model.get("modelId")}
    tl = data.get("thoughtLevel") or None

    if purpose in ("project", "task") and os.path.isdir(ws_path):
        ensure_workspace_registered(ws_path, purpose)

    try:
        res, sid = await runner.create(
            {"workspaceKey": ws_path, "workspacePath": ws_path},
            mode=mode, model=model, thought_level=tl)
    except Exception as exc:
        return json_error(f"新建会话失败: {exc}", 502)

    log(f"新建会话: {sid} 工作区={ws_path} 模式={mode} 思考={tl}")
    return web.json_response({"ok": True, "sessionId": sid,
                              "session": (res or {}).get("session") or {}})


async def handle_session_messages(request):
    blocked = guard(request)
    if blocked:
        return blocked
    sid = request.match_info["sid"]
    if not SESSION_ID_RE.match(sid):
        return json_error("非法的会话 ID", 400)
    row = session_row(sid)
    if not row:
        return json_error("会话不存在", 404)
    msgs = load_messages(sid)
    f = feed.get(sid)
    return web.json_response({
        "ok": True,
        "sessionId": sid,
        "title": row.get("title") or "",
        "workspacePath": row.get("directory") or "",
        "archived": bool(row.get("time_archived")),
        "messages": msgs,
        "live": bool(f and f.turn_active),
        "usage": (f.usage if f else None),
    })


async def handle_session_archive(request):
    blocked = guard(request)
    if blocked:
        return blocked
    sid = request.match_info["sid"]
    if not SESSION_ID_RE.match(sid):
        return json_error("非法的会话 ID", 400)
    if runner.is_running(sid):
        return json_error("会话正在执行中，请先停止", 409)
    data = await body_json(request)
    archived = bool(data.get("archived", True))
    n = archive_session(sid, archived)
    # 协议层会缓存会话，改库后需要重启 app-server 才能反映
    await zc.restart("会话归档状态变更")
    log(f"{'归档' if archived else '取消归档'}会话: {sid}（rowcount={n}）")
    return web.json_response({"ok": True, "archived": archived, "rowcount": n})


async def handle_session_delete(request):
    blocked = guard(request)
    if blocked:
        return blocked
    sid = request.match_info["sid"]
    if not SESSION_ID_RE.match(sid):
        return json_error("非法的会话 ID", 400)
    if runner.is_running(sid):
        return json_error("会话正在执行中，请先停止", 409)
    n = delete_session(sid)
    feed.feeds.pop(sid, None)
    await zc.restart("会话已删除")
    log(f"删除会话: {sid}（rowcount={n}）")
    return web.json_response({"ok": True})


async def handle_session_settings(request):
    """改会话的模式 / 模型 / 思考等级。"""
    blocked = guard(request)
    if blocked:
        return blocked
    sid = request.match_info["sid"]
    if not SESSION_ID_RE.match(sid):
        return json_error("非法的会话 ID", 400)
    if runner.is_running(sid):
        return json_error("会话正在执行中，请稍候再改", 409)
    data = await body_json(request)
    mode = data.get("mode")
    model = data.get("model")
    tl = data.get("thoughtLevel")
    if mode and mode not in VALID_MODES:
        return json_error("非法模式")
    if isinstance(model, dict):
        model = {"providerId": model.get("providerId"), "modelId": model.get("modelId")}
    try:
        await runner.resume(sid)
        await runner.apply_settings(sid, mode=mode, model=model, thought_level=tl)
    except Exception as exc:
        return json_error(f"设置失败: {exc}", 502)
    return web.json_response({"ok": True})


# ---------------------------------------------------------------- 路由：设置

async def handle_settings_security(request):
    blocked = guard(request)
    if blocked:
        return blocked
    return web.json_response({
        "ok": True,
        "key": current_login_key(),
        "isDefault": not config.get("key_sha256"),
        "ip": client_ip(request),
        "maxAttempts": int(config.get("max_attempts", 5)),
        "autoApprove": bool(config.get("auto_approve", True)),
        "defaultMode": config.get("default_mode", "build"),
        "workspaceRoot": config.get("workspace_root", "/root"),
        "permissions": (zc.permission_events if zc else [])[-20:],
    })


async def handle_settings_key(request):
    """改登录密钥。不需要提供原密钥。"""
    blocked = guard(request)
    if blocked:
        return blocked
    data = await body_json(request)
    newkey = str(data.get("key") or "").strip()
    if len(newkey) < 6:
        return json_error("密钥至少 6 位")
    if len(newkey) > 128:
        return json_error("密钥过长")
    config["key_plain"] = newkey
    config["key_sha256"] = hash_key(newkey)
    save_config()
    log("登录密钥已更新")
    return web.json_response({"ok": True, "key": newkey})


async def handle_settings_misc(request):
    """自动放行 / 最大尝试次数等开关。"""
    blocked = guard(request)
    if blocked:
        return blocked
    data = await body_json(request)
    if "autoApprove" in data:
        config["auto_approve"] = bool(data["autoApprove"])
        if zc:
            zc.auto_approve = config["auto_approve"]
    if "maxAttempts" in data:
        try:
            n = int(data["maxAttempts"])
            if 1 <= n <= 100:
                config["max_attempts"] = n
        except Exception:
            pass
    if "defaultMode" in data and data["defaultMode"] in VALID_MODES:
        config["default_mode"] = data["defaultMode"]
    save_config()
    return web.json_response({"ok": True, "config": {
        "autoApprove": config.get("auto_approve"),
        "maxAttempts": config.get("max_attempts"),
        "defaultMode": config.get("default_mode"),
    }})


async def handle_settings_providers(request):
    """列出厂商配置。默认不返回明文密钥，除非显式 reveal。"""
    blocked = guard(request)
    if blocked:
        return blocked
    reveal = request.query.get("reveal") == "1"
    items = provider_list(with_secrets=reveal)
    cfg = read_zcode_config()
    return web.json_response({
        "ok": True,
        "providers": items,
        "official": [p for p in items if p["scope"] == "official"],
        "custom": [p for p in items if p["scope"] == "custom"],
        "mainModel": (cfg.get("model") or {}).get("main") or "",
        "configPath": str(ZCODE_CONFIG),
    })


async def handle_provider_upsert(request):
    blocked = guard(request)
    if blocked:
        return blocked
    data = await body_json(request)
    pid = str(data.get("providerId") or "").strip()
    if not pid or not PROVIDER_ID_RE.match(pid):
        return json_error("厂商 ID 不合法（字母数字下划线横线，1-40 位）")
    async with zc.config_lock:
        cfg = read_zcode_config()
        providers = cfg.setdefault("provider", {})
        entry = build_provider_entry(data, providers.get(pid))
        if not (entry.get("options") or {}).get("baseURL"):
            return json_error("必须填写 Base URL")
        if not entry.get("models"):
            return json_error("该厂商下至少要有一个模型，请在弹窗的模型列表里添加")
        providers[pid] = entry
        write_zcode_config(cfg)
    await zc.restart("厂商配置变更")
    log(f"保存厂商: {pid}")
    return web.json_response({"ok": True, "providerId": pid})


async def handle_provider_delete(request):
    blocked = guard(request)
    if blocked:
        return blocked
    pid = request.match_info["pid"]
    async with zc.config_lock:
        cfg = read_zcode_config()
        providers = cfg.get("provider") or {}
        if pid not in providers:
            return json_error("厂商不存在", 404)
        providers.pop(pid)
        cfg["provider"] = providers
        # 如果当前主模型属于它，换一个
        main = (cfg.get("model") or {}).get("main") or ""
        if main.startswith(pid + "/"):
            rest = [p for p in providers]
            cfg["model"] = {"main": "", "lite": ""}
            if rest:
                first_pid = rest[0]
                first_mid = list((providers[first_pid].get("models") or {}).keys())
                if first_mid:
                    cfg["model"] = {"main": "%s/%s" % (first_pid, first_mid[0]),
                                    "lite": "%s/%s" % (first_pid, first_mid[0])}
        write_zcode_config(cfg)
    await zc.restart("厂商已删除")
    log(f"删除厂商: {pid}")
    return json_response_ok()


def json_response_ok(**extra):
    return web.json_response({"ok": True, **extra})


async def handle_model_upsert(request):
    blocked = guard(request)
    if blocked:
        return blocked
    pid = request.match_info["pid"]
    data = await body_json(request)
    mid = str(data.get("modelId") or "").strip()
    if not mid:
        return json_error("模型 ID 必填")
    async with zc.config_lock:
        cfg = read_zcode_config()
        providers = cfg.get("provider") or {}
        if pid not in providers:
            return json_error("厂商不存在", 404)
        models = providers[pid].setdefault("models", {})
        m = dict(models.get(mid) or {})
        m["name"] = str(data.get("name") or m.get("name") or mid)
        lim = dict(m.get("limit") or {})
        for k, field in (("context", "context"), ("output", "output")):
            if data.get(k) not in (None, ""):
                try:
                    lim[field] = int(data[k])
                except Exception:
                    pass
        if lim:
            m["limit"] = lim
        mod = dict(m.get("modalities") or {})
        if data.get("supportsImages") is not None:
            mod["input"] = (["text", "image"] if data["supportsImages"] else ["text"])
            mod["output"] = ["text"]
        if mod:
            m["modalities"] = mod
        models[mid] = m
        write_zcode_config(cfg)
    await zc.restart("模型配置变更")
    log(f"保存模型: {pid}/{mid}")
    return json_response_ok(modelId=mid)


async def handle_model_delete(request):
    blocked = guard(request)
    if blocked:
        return blocked
    pid = request.match_info["pid"]
    mid = request.match_info["mid"]
    async with zc.config_lock:
        cfg = read_zcode_config()
        providers = cfg.get("provider") or {}
        if pid not in providers:
            return json_error("厂商不存在", 404)
        models = providers[pid].get("models") or {}
        if mid not in models:
            return json_error("模型不存在", 404)
        if len(models) <= 1:
            return json_error("每个厂商至少保留一个模型")
        models.pop(mid)
        main = (cfg.get("model") or {}).get("main") or ""
        if main == "%s/%s" % (pid, mid):
            nxt = list(models.keys())[0]
            cfg["model"] = {"main": "%s/%s" % (pid, nxt), "lite": "%s/%s" % (pid, nxt)}
        write_zcode_config(cfg)
    await zc.restart("模型已删除")
    return json_response_ok()


async def handle_active_model(request):
    blocked = guard(request)
    if blocked:
        return blocked
    data = await body_json(request)
    pid = str(data.get("providerId") or "").strip()
    mid = str(data.get("modelId") or "").strip()
    async with zc.config_lock:
        cfg = read_zcode_config()
        providers = cfg.get("provider") or {}
        if pid not in providers or mid not in (providers[pid].get("models") or {}):
            return json_error("模型不存在", 404)
        full = "%s/%s" % (pid, mid)
        cfg["model"] = {"main": full, "lite": full}
        write_zcode_config(cfg)
    await zc.restart("默认模型变更")
    log(f"默认模型设为 {full}")
    return json_response_ok(main=full)


# ---------------------------------------------------------------- WebSocket

async def handle_ws(request):
    ip = client_ip(request)
    ban_store.reload_if_changed()
    if not ip_allowed(ip):
        return web.Response(status=403, text="forbidden")
    if ban_store.is_banned(ip):
        return web.Response(status=403, text="banned")
    if not is_authed(request):
        return web.Response(status=401, text="unauthorized")

    ws = web.WebSocketResponse(heartbeat=30, max_msg_size=4 * 1024 * 1024)
    await ws.prepare(request)

    watched = set()      # 本连接订阅的 sessionId
    await safe_send(ws, {"type": "ready", "appServer": bool(zc and zc.proc and zc.proc.returncode is None)})

    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                if msg.type == WSMsgType.ERROR:
                    break
                continue
            try:
                data = json.loads(msg.data)
            except Exception:
                await safe_send(ws, {"type": "error", "message": "消息格式错误"})
                continue

            action = data.get("type")

            if action == "ping":
                await safe_send(ws, {"type": "pong"})
                continue

            if action == "watch":
                sid = data.get("sessionId")
                if sid and SESSION_ID_RE.match(sid):
                    f = feed.get(sid)
                    if f:
                        f.add_subscriber(ws)
                        watched.add(sid)
                        # 把已缓存的事件回放给迟到者
                        for ev in f.buffer[-400:]:
                            await safe_send(ws, ev)
                        if f.turn_active:
                            await safe_send(ws, {"type": "resumed", "sessionId": sid})
                continue

            if action == "unwatch":
                sid = data.get("sessionId")
                f = feed.get(sid) if sid else None
                if f:
                    f.remove_subscriber(ws)
                watched.discard(sid)
                continue

            if action == "cancel":
                sid = data.get("sessionId")
                if not sid or not SESSION_ID_RE.match(sid):
                    await safe_send(ws, {"type": "error", "message": "非法的会话 ID"})
                    continue
                if await runner.cancel(sid):
                    await safe_send(ws, {"type": "cancelling", "sessionId": sid})
                else:
                    await safe_send(ws, {"type": "error", "message": "当前没有正在执行的对话"})
                continue

            if action != "send":
                continue

            sid = data.get("sessionId") or None
            content = str(data.get("content") or data.get("message") or "").strip()
            if not sid:
                await safe_send(ws, {"type": "error",
                                     "message": "请先新建会话再发送消息"})
                continue
            if not content:
                await safe_send(ws, {"type": "error", "message": "消息不能为空"})
                continue
            if len(content) > MAX_MESSAGE_CHARS:
                await safe_send(ws, {"type": "error",
                                     "message": f"消息过长（上限 {MAX_MESSAGE_CHARS} 字）"})
                continue
            if sid and not SESSION_ID_RE.match(sid):
                await safe_send(ws, {"type": "error", "message": "非法的会话 ID"})
                continue
            # 注意：`session/create` 建出来的会话要等第一轮跑完才落库，
            # 所以不能只查库，否则刚建的会话第一条消息会被误判成「不存在」。
            if sid and not (session_row(sid) or sid in runner.desired):
                await safe_send(ws, {"type": "error", "message": "会话不存在，请新建一个"})
                continue

            mode = data.get("mode")
            if mode and mode not in VALID_MODES:
                mode = None
            model = data.get("model")
            if isinstance(model, dict):
                model = {"providerId": model.get("providerId"), "modelId": model.get("modelId")}
            else:
                model = None
            tl = data.get("thoughtLevel") or None

            if runner.is_running(sid):
                await safe_send(ws, {"type": "error",
                                     "message": "这个会话正在执行中，请稍候", "sessionId": sid})
                continue

            try:
                f = await feed.ensure_subscription(sid)
                f.add_subscriber(ws)
                watched.add(sid)
                await runner.send(sid, content, mode=mode, model=model, thought_level=tl)
                await safe_send(ws, {"type": "accepted", "sessionId": sid})
            except Exception as exc:
                log(f"发送失败: {exc}")
                await safe_send(ws, {"type": "error", "message": f"发送失败: {exc}", "sessionId": sid})

    except Exception as exc:
        log(f"WebSocket 异常: {exc}")
    finally:
        for sid in watched:
            f = feed.get(sid)
            if f:
                f.remove_subscriber(ws)
        try:
            await ws.close()
        except Exception:
            pass

    return ws


async def safe_send(ws, payload):
    try:
        if not ws.closed:
            await ws.send_json(payload)
    except Exception:
        pass


# ---------------------------------------------------------------- 生命周期

START_TIME = time.time()


async def on_startup(app):
    global ban_store, token_store, zc, feed, runner
    load_config()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ban_store = BanStore(BAN_PATH)
    token_store = TokenStore(int(config.get("token_ttl", 43200)))
    zc = ZCodeServer()
    zc.auto_approve = bool(config.get("auto_approve", True))
    feed = SessionFeedManager()
    runner = TurnManager()
    log("封禁名单已载入：%d 条记录，其中永久封禁 %d 个"
        % (len(ban_store.data), sum(1 for v in ban_store.data.values() if v.get("banned"))))
    try:
        await zc.start()
    except Exception as exc:
        log(f"app-server 启动失败: {exc}")
    # 后台重建 provider registry + 预热模型目录（readState 首次调用要等 MCP 初始化，很慢）
    asyncio.create_task(bootstrap_catalog("服务启动"))
    log("监听 %s:%s，默认模式 %s" % (config.get("bind"), config.get("port"),
                                    config.get("default_mode")))
    log("zcode 配置：%s" % ZCODE_CONFIG)


async def on_cleanup(app):
    if zc:
        await zc.stop()


def make_app():
    app = web.Application(client_max_size=8 * 1024 * 1024)
    app.router.add_get("/", handle_index)
    app.router.add_get("/healthz", handle_health)
    app.router.add_post("/api/login", handle_login)
    app.router.add_post("/api/logout", handle_logout)
    app.router.add_get("/api/me", handle_me)
    app.router.add_get("/api/options", handle_options)

    app.router.add_get("/api/workspaces", handle_workspaces)
    app.router.add_post("/api/workspaces", handle_workspace_create)
    app.router.add_delete("/api/workspaces/{wid}", handle_workspace_delete)

    app.router.add_get("/api/sessions", handle_sessions)
    app.router.add_post("/api/sessions", handle_session_create)
    app.router.add_get("/api/sessions/{sid}/messages", handle_session_messages)
    app.router.add_post("/api/sessions/{sid}/archive", handle_session_archive)
    app.router.add_delete("/api/sessions/{sid}", handle_session_delete)
    app.router.add_post("/api/sessions/{sid}/settings", handle_session_settings)

    app.router.add_get("/api/settings/security", handle_settings_security)
    app.router.add_post("/api/settings/key", handle_settings_key)
    app.router.add_post("/api/settings/misc", handle_settings_misc)
    app.router.add_get("/api/settings/providers", handle_settings_providers)
    app.router.add_post("/api/settings/providers", handle_provider_upsert)
    app.router.add_delete("/api/settings/providers/{pid}", handle_provider_delete)
    app.router.add_post("/api/settings/providers/{pid}/models", handle_model_upsert)
    app.router.add_delete("/api/settings/providers/{pid}/models/{mid}", handle_model_delete)
    app.router.add_post("/api/settings/active-model", handle_active_model)

    app.router.add_get("/ws", handle_ws)
    app.router.add_static("/static/", STATIC_DIR)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    load_config()
    web.run_app(make_app(),
                host=config.get("bind", "0.0.0.0"),
                port=int(config.get("port", 8081)),
                access_log=None,
                print=None)
