# -*- coding: utf-8 -*-
"""本地校验「后台保活 + 不被其他操作打断」这两条后端逻辑。

只测纯逻辑（不连服务器）：
  1. remote_turn_alive —— 事件静默时向 app-server 求证，绝不能把还在跑的任务判成结束
  2. restart_guard    —— 会重启 app-server 的操作，在有人跑任务时必须被挡下
"""
import asyncio
import importlib.util
import os
import sys
import types

# ---- aiohttp 桩 ----
aiohttp = types.ModuleType("aiohttp")


class _Web(object):
    def __getattr__(self, name):
        return lambda *a, **k: None


aiohttp.web = _Web()
aiohttp.WSMsgType = types.SimpleNamespace(TEXT=1, ERROR=2)
sys.modules["aiohttp"] = aiohttp

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("panel_app", os.path.join(HERE, "app.py"))
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)

FAIL = []


def check(label, got, want):
    ok = got == want
    print("%-60s %s" % (label, "OK " if ok else "FAIL got=%r want=%r" % (got, want)))
    if not ok:
        FAIL.append(label)


# json_error 需要能拿到 status 才能断言，把桩换成会记账的版本
app.web.json_response = lambda payload=None, status=200, **k: {"status": status,
                                                              "payload": payload}


class FakeZc(object):
    def __init__(self, result=None, boom=False):
        self.result = result
        self.boom = boom
        self.calls = []

    async def call(self, method, params=None, timeout=None):
        self.calls.append(method)
        if self.boom:
            raise RuntimeError("app-server 不可达")
        return self.result


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def sessions(*pairs):
    return {"sessions": [{"sessionId": sid, "status": st} for sid, st in pairs]}


print("=== 1. 事件静默时的求证（后台保活的命门）===")

# 会话在跑：绝不能判成结束
app.zc = FakeZc(sessions(("sess_a", "running")))
check("app-server 说 running -> 认为还在跑", run(app.remote_turn_alive("sess_a")), True)

for st in ("active", "busy", "working", "executing", "in_progress", "RUNNING"):
    app.zc = FakeZc(sessions(("sess_a", st)))
    check("状态 %r -> 认为还在跑" % st, run(app.remote_turn_alive("sess_a")), True)

# 会话确实闲着：可以收工
for st in ("idle", "completed", "done", "stopped", "cancelled", "canceled",
           "closed", "archived", "error", "failed", "IDLE"):
    app.zc = FakeZc(sessions(("sess_a", st)))
    check("状态 %r -> 判定结束" % st, run(app.remote_turn_alive("sess_a")), False)

# 列表里查不到：新建会话要等第一轮跑完才落库，查不到必须保守当「还在跑」
app.zc = FakeZc(sessions(("sess_other", "idle")))
check("列表里没有该会话 -> 保守认为还在跑", run(app.remote_turn_alive("sess_a")), True)
app.zc = FakeZc({"sessions": []})
check("列表为空 -> 保守认为还在跑", run(app.remote_turn_alive("sess_a")), True)

# 查询本身失败：同样不能误判成结束
app.zc = FakeZc(sessions(("sess_a", "idle")), boom=True)
check("查询异常 -> 保守认为还在跑", run(app.remote_turn_alive("sess_a")), True)

# 状态字段缺失：不算在「结束」清单里，当作还在跑
app.zc = FakeZc({"sessions": [{"sessionId": "sess_a"}]})
check("状态字段缺失 -> 认为还在跑", run(app.remote_turn_alive("sess_a")), True)
app.zc = FakeZc({"sessions": [{"sessionId": "sess_a", "status": None}]})
check("状态为 null -> 认为还在跑", run(app.remote_turn_alive("sess_a")), True)


print()
print("=== 1b. 三态查询（挡住用户发消息时不该乱挡，所以需要区分 unknown）===")
app.zc = FakeZc(sessions(("sess_a", "running")))
check("running -> active", run(app.remote_turn_state("sess_a")), "active")
app.zc = FakeZc(sessions(("sess_a", "idle")))
check("idle -> idle", run(app.remote_turn_state("sess_a")), "idle")
app.zc = FakeZc(sessions(("sess_other", "idle")))
check("列表里没有 -> unknown", run(app.remote_turn_state("sess_a")), "unknown")
app.zc = FakeZc(sessions(("sess_a", "idle")), boom=True)
check("查询异常 -> unknown", run(app.remote_turn_state("sess_a")), "unknown")


print()
print("=== 2. 硬上限（不能让跟丢的任务永久占着会话）===")
check("硬上限远大于静默阈值", app.TURN_ABSOLUTE_MAX > app.TURN_IDLE_TIMEOUT * 4, True)
check("硬上限存在且有限", 0 < app.TURN_ABSOLUTE_MAX <= 24 * 3600, True)
check("SessionFeed 有本轮起始时间字段", hasattr(app.SessionFeed("s_x"), "turn_started_at"), True)


print()
print("=== 3. 重启守卫：不许别的操作打断正在跑的任务 ===")


class FakeRunner(object):
    def __init__(self, running=()):
        self.running = {s: {} for s in running}

    def is_running(self, sid):
        return sid in self.running


app.runner = FakeRunner()
check("没人执行 -> running_sessions 为空", app.running_sessions(), [])
check("没人执行 -> 守卫放行", app.restart_guard(), None)

app.runner = FakeRunner(["sess_a"])
check("1 个会话在跑 -> 守卫拦下", (app.restart_guard() or {}).get("status"), 409)
check("拦下时带上「还有几个」的说明",
      "1" in ((app.restart_guard() or {}).get("payload") or {}).get("error", ""), True)
check("排除自己后放行（归档/删除自己那条已经单独挡过）",
      app.restart_guard("sess_a"), None)
check("但别的会话在跑时仍然要拦",
      (app.restart_guard("sess_b") or {}).get("status"), 409)

app.runner = FakeRunner(["sess_a", "sess_b", "sess_c"])
check("3 个会话在跑 -> 拦下并说明数量",
      "3" in ((app.restart_guard() or {}).get("payload") or {}).get("error", ""), True)
check("running_sessions 返回全部", len(app.running_sessions()), 3)

app.runner = None
check("runner 还没初始化时不炸", app.running_sessions(), [])
check("runner 为 None 时守卫放行", app.restart_guard(), None)


print()
print("=== 4. 只有「停止」能打断（其它路径不得触碰 cancel）===")
import io as _io
src = _io.open(os.path.join(HERE, "app.py"), encoding="utf-8").read()
# handle_ws 的 cancel 只认前端显式点「停止生成」
check("WS 的 cancel 分支存在（停止按钮的唯一入口）", 'if action == "cancel":' in src, True)
# 调度器是用户自己配的定时切换，属于刻意行为，不走 restart_guard
check("定时调度器仍可主动打断并续跑", "await runner.cancel(sid, strict=True)" in src, True)


print()
print("=== 5. 点停之后立刻发消息：不许静默丢 ===")
# 这段是 I/O 密集的，直接校验代码形态（行为已由端到端验证覆盖）
check("点停时记下「刚请求过停止」", "f.stop_requested = True" in src, True)
check("发消息前会跟 app-server 对账",
      "f0.stop_requested" in src and "await remote_turn_state(session_id)" in src, True)
check("对账不掉时如实报错，而不是假装成功",
      "上一轮还没真正停下来" in src, True)
check("新一轮开始时清掉该标记", "f.stop_requested = False     # 新一轮开始" in src, True)
# 乐观置位只在面板这一侧发生；send 仍在 start_pump 之前拦截，不会污染 pump
check("对账发生在 start_pump 之前（失败时不留半截状态）",
      src.index("上一轮还没真正停下来") < src.index("await feed.start_pump(session_id)"), True)


print()
print("=== 6. app-server 连接健壮性（读线程死掉不能变成哑巴）===")
# 实测踩到过：asyncio 默认单行上限 64KB，会话一长（冷启动 resume 重放整段历史）
# 就抛 LimitOverrunError 把读线程打死，进程还在但再也没人读它的回答。
check("单行 JSON 上限远大于 asyncio 默认的 64KB", app.MAX_RPC_LINE >= 4 * 1024 * 1024, True)
check("创建子进程时把上限传进去了", "limit=MAX_RPC_LINE" in src, True)

_srv = app.ZCodeServer()
check("默认未标记为断开", _srv.reader_dead, False)
_srv.reader_dead = True
try:
    run(_srv.call("session/list"))
    check("连接已废时立刻报错", "没有抛错", "RuntimeError")
except RuntimeError as exc:
    check("连接已废时立刻报错（不再白等 90s 超时）", "连接已失效" in str(exc), True)

check("读线程异常时标记连接已废", "self.reader_dead = True" in src, True)
check("没人在跑任务时自动重建连接",
      "asyncio.create_task(self._recover_reader())" in src
      and "if not (runner and runner.running)" in src, True)
check("连续拉取失败有上限（不无限刷日志）", "fails >= EVENT_FAIL_MAX" in src, True)
check("拉取成功会清零失败计数", "                fails = 0" in src, True)


print()
print("=== 7. 异步删除会话：session/close + 删库，绝不重启 app-server ===")

# ---- 桩准备 ----
class FakeReq(object):
    def __init__(self, sid):
        self.match_info = {"sid": sid}


class FakeFeed(object):
    def __init__(self):
        self.feeds = {}


class FakeRunner2(FakeRunner):
    def __init__(self, running=()):
        FakeRunner.__init__(self, running)
        self.desired = {}
        self.pending_settings = {}
        self.auto_run = {}
        self.last_sent = {}
        self.active = set()
        self.forgot = []

    def forget(self, sid):
        self.forgot.append(sid)
        self.desired.pop(sid, None)
        self.pending_settings.pop(sid, None)
        self.auto_run.pop(sid, None)
        self.last_sent.pop(sid, None)
        self.active.discard(sid)


class FakeZcDel(FakeZc):
    def __init__(self, result=None, boom=False):
        FakeZc.__init__(self, result, boom)
        self.restarts = 0

    async def restart(self, reason=""):
        self.restarts += 1


app.guard = lambda req: None
app.delete_session = lambda sid: 1
app.cleanup_session_sidecars = lambda sid: {"files": 0, "cron": 0}

app.feed = FakeFeed()
app.runner = FakeRunner2()
app.runner.desired["sess_xxxx1234567890"] = {"mode": "build"}
app.runner.active.add("sess_xxxx1234567890")
app.runner.auto_run["sess_xxxx1234567890"] = {"enabled": True}
zcd = FakeZcDel({"closed": True})
app.zc = zcd
res = run(app.handle_session_delete(FakeReq("sess_xxxx1234567890")))
check("其他会话没在跑 + 目标闲 -> 放行 200", res["status"], 200)
check("先调 session/close（实测 close 即从内存卸载）",
      zcd.calls[:1] == ["session/close"], True)
check("绝不调用 zc.restart", zcd.restarts, 0)
check("runner 内存状态被 forget 清掉", app.runner.forgot, ["sess_xxxx1234567890"])
check("desired 清了", "sess_xxxx1234567890" in app.runner.desired, False)
check("auto_run 清了", "sess_xxxx1234567890" in app.runner.auto_run, False)
check("响应带 closed 标记", (res["payload"] or {}).get("closed"), True)

# close 失败（app-server 报 not active 等）也要照删
app.runner = FakeRunner2()
zcd2 = FakeZcDel(boom=True)
app.zc = zcd2
res = run(app.handle_session_delete(FakeReq("sess_yyyy1234567890")))
check("close 失败 -> 仍然删库成功", res["status"], 200)
check("close 失败也绝不重启", zcd2.restarts, 0)
check("close 失败信息回传", (res["payload"] or {}).get("closeError") is not None, True)

# 目标会话自己在跑：还是必须先停止（这是正当保护，不是重启门闸）
app.runner = FakeRunner2(running=("sess_zzzz1234567890",))
zcd3 = FakeZcDel({"closed": True})
app.zc = zcd3
res = run(app.handle_session_delete(FakeReq("sess_zzzz1234567890")))
check("目标会话在跑 -> 409 先停止", res["status"], 409)
check("409 时连 close 都不发", zcd3.calls, [])

# 关键回归：其他会话在跑时，删除不再被「重启门闸」挡下（这正是本次改造的意义）
app.runner = FakeRunner2(running=("sess_other_task",))
zcd4 = FakeZcDel({"closed": True})
app.zc = zcd4
res = run(app.handle_session_delete(FakeReq("sess_tgt1234567890")))
check("其他会话在跑 -> 删除照样放行（不再 409）", res["status"], 200)
check("整个过程零重启", zcd4.restarts, 0)


print()
print("FAILED: %d %s" % (len(FAIL), FAIL if FAIL else ""))
sys.exit(1 if FAIL else 0)
