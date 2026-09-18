# -*- coding: utf-8 -*-
"""本地校验「添加模型时可测试 / 可拉模型列表 / 每会话独立模型」的后端逻辑。

不连服务器、不发真实请求：probe_http 用桩替换成按 URL 返回固定响应。
重点是把用户实际踩到的那个坑复现成断言 ——
    kind=openai-compatible + Base URL=…/api/anthropic → 厂商在 HTTP 200 里回 404 NOT_FOUND
"""
import asyncio
LOOP = asyncio.new_event_loop()
import importlib.util
import os
import sys
import types

# ---- aiohttp 桩（app.py 顶层 import aiohttp；probe_http 内部才会真正用它）----
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
    print("%-62s %s" % (label, "OK " if ok else "FAIL got=%r want=%r" % (got, want)))
    if not ok:
        FAIL.append(label)


def has(label, needle, hay):
    ok = needle in (hay or "")
    print("%-62s %s" % (label, "OK " if ok else "FAIL %r 不在 %r" % (needle, hay)))
    if not ok:
        FAIL.append(label)


print("=== 1. 纯函数：路径候选 ===")
c = app.probe_candidates("openai-compatible", "https://open.bigmodel.cn/api/anthropic")
check("openai+anthropic路径 -> 第1个按配置走",
      c[0], "https://open.bigmodel.cn/api/anthropic/chat/completions")
check("openai+anthropic路径 -> 第2个是厂商正确地址",
      c[1], "https://open.bigmodel.cn/api/paas/v4/chat/completions")

c2 = app.probe_candidates("anthropic", "https://open.bigmodel.cn/api/paas/v4")
check("anthropic+paas路径 -> 第1个按配置走",
      c2[0], "https://open.bigmodel.cn/api/paas/v4/v1/messages")
check("anthropic+paas路径 -> 第2个是厂商 anthropic 地址",
      c2[1], "https://open.bigmodel.cn/api/anthropic/v1/messages")

c3 = app.probe_candidates("openai-compatible", "https://api.aweqy.top/v1")
check("已是正确配置 -> 第1个即正确", c3[0], "https://api.aweqy.top/v1/chat/completions")

check("Base URL 反推（openai）", app.probe_base_for(
    "openai-compatible", "https://open.bigmodel.cn/api/paas/v4/chat/completions"),
    "https://open.bigmodel.cn/api/paas/v4")
check("Base URL 反推（anthropic）", app.probe_base_for(
    "anthropic", "https://open.bigmodel.cn/api/anthropic/v1/messages"),
    "https://open.bigmodel.cn/api/anthropic")

print()
print("=== 2. 响应体判定：HTTP 200 里的错误必须算失败 ===")
r = app.probe_verdict("openai-compatible", 200,
                      '{"code":500,"msg":"404 NOT_FOUND","success":false}')
check("智谱 200+404 NOT_FOUND -> ok=False", r[0], False)
check("  detail 带出厂商原话", r[1], "[500] 404 NOT_FOUND")
r = app.probe_verdict("openai-compatible", 200,
                      '{"choices":[{"message":{"content":"收到"}}]}')
check("标准 openai 成功 -> ok=True", r[0], True)
check("  取到回复", r[1], "收到")
r = app.probe_verdict("anthropic", 200,
                      '{"type":"message","content":[{"type":"text","text":"收到"}]}')
check("anthropic 成功 -> ok=True", r[0], True)
check("  取到回复", r[1], "收到")
r = app.probe_verdict("openai-compatible", 401, '{"error":{"message":"令牌已过期"}}')
check("401 错误体 -> ok=False", r[0], False)
r = app.probe_verdict("openai-compatible", 429,
                      '{"error":{"code":"1113","message":"余额不足或无可用资源包,请充值。"}}')
check("429 余额不足 -> ok=False", r[0], False)
has("  提示指向余额", "余额不足", app.probe_hint(r[1], 429))
has("404 的提示指向路径", "路径不对", app.probe_hint("404 NOT_FOUND", 200))
has("令牌的提示指向 Key", "API Key", app.probe_hint("令牌已过期", 401))

print()
print("=== 3. 场景复现：用户那套配置（openai-compatible + /api/anthropic）===")
CALLS = []


def fake_http(responses):
    """responses: {url: (status, text)}，未命中的按 404 处理。"""
    async def _f(url, body=None, headers=None, timeout=None):
        CALLS.append(url)
        if url in responses:
            st, tx = responses[url]
            return {"ok": True, "status": st, "text": tx, "url": url, "latencyMs": 15}
        return {"ok": True, "status": 404, "text": '{"error":"not found"}',
                "url": url, "latencyMs": 10}
    return _f


# 3a. 现状：/api/anthropic 上打 chat/completions 报 404（塞在 200 里），
#     而厂商真正的 openai 端点 /api/paas/v4 能打到（只是余额不足）
app.probe_http = fake_http({
    "https://open.bigmodel.cn/api/anthropic/chat/completions":
        (200, '{"code":500,"msg":"404 NOT_FOUND","success":false}'),
    "https://open.bigmodel.cn/api/paas/v4/chat/completions":
        (200, '{"error":{"code":"1113","message":"余额不足或无可用资源包,请充值。"}}'),
})
res = LOOP.run_until_complete(
    app.probe_chat("openai-compatible", "https://open.bigmodel.cn/api/anthropic",
                   "sk-x", "GLM-5.3-Flash"))
check("结论：不通", res["ok"], False)
check("结论：判定「换个 Base URL 就能打到厂商」", res.get("pathOk"), True)
check("结论：给出该改成的地址",
      res.get("suggestBaseURL"), "https://open.bigmodel.cn/api/paas/v4")
has("结论：第二层原因（余额）也带出来了", "余额不足", res.get("hint"))
check("先试了配置里的地址", CALLS[0],
      "https://open.bigmodel.cn/api/anthropic/chat/completions")

# 3b. 只把 Base URL 改对（Key 有效、额度正常）-> 必须报通
CALLS.clear()
app.probe_http = fake_http({
    "https://open.bigmodel.cn/api/paas/v4/chat/completions":
        (200, '{"choices":[{"message":{"content":"收到"}}]}'),
})
res = LOOP.run_until_complete(
    app.probe_chat("openai-compatible", "https://open.bigmodel.cn/api/paas/v4",
                   "sk-x", "glm-5.3-flash"))
check("改对后：通", res["ok"], True)
check("改对后：回复可见", res["reply"], "收到")
check("改对后：不需要再建议改地址", "suggestBaseURL" in res, False)

# 3c. Key 无效 -> 立刻收工，不该继续换路径刷请求
CALLS.clear()
app.probe_http = fake_http({
    "https://open.bigmodel.cn/api/paas/v4/chat/completions":
        (401, '{"error":{"message":"令牌已过期或验证不正确"}}'),
})
res = LOOP.run_until_complete(
    app.probe_chat("openai-compatible", "https://open.bigmodel.cn/api/paas/v4",
                   "bad", "glm-5.3-flash"))
check("Key 错：不通", res["ok"], False)
check("Key 错：只发了一次请求", len(CALLS), 1)
has("Key 错：提示指向 Key", "API Key", res.get("hint"))

# 3d. 网络不通 -> 提示网络
CALLS.clear()


async def dead(url, body=None, headers=None, timeout=None):
    CALLS.append(url)
    return {"ok": False, "status": 0, "url": url, "timeout": True,
            "error": "请求超时（30 秒）—— 网络到不了这个地址"}


app.probe_http = dead
res = LOOP.run_until_complete(
    app.probe_chat("openai-compatible", "https://api.aweqy.top/v1", "sk", "m"))
check("网络不通：不通", res["ok"], False)
has("网络不通：提示网络", "网络", res.get("error") or res.get("hint"))

print()
print("=== 4. 拉模型列表 ===")
app.probe_http = fake_http({
    "https://open.bigmodel.cn/api/anthropic/models":
        (404, '{"error":"not found"}'),
    "https://open.bigmodel.cn/api/paas/v4/models":
        (200, '{"data":[{"id":"glm-5.3-flash","name":"GLM-5.3-Flash"},'
              '{"id":"glm-4-flash"}]}'),
})
res = LOOP.run_until_complete(
    app.probe_model_list("openai-compatible", "https://open.bigmodel.cn/api/anthropic",
                         "sk-x"))
check("拉到列表", res["ok"], True)
check("  条数", res["count"], 2)
check("  第一条 id", res["models"][0]["id"], "glm-5.3-flash")
check("  缺 name 时回落成 id", res["models"][1]["name"], "glm-4-flash")
check("  顺带指出列表在哪个地址上",
      res.get("suggestBaseURL"), "https://open.bigmodel.cn/api/paas/v4")

app.probe_http = fake_http({
    "https://api.aweqy.top/v1/models":
        (200, '{"object":"list","data":[{"id":"z-ai/glm-5.3-flash"},'
              '{"id":"deepseek-v4-flash"}]}'),
})
res = LOOP.run_until_complete(
    app.probe_model_list("openai-compatible", "https://api.aweqy.top/v1", "sk-x"))
check("自建网关也能拉", res["models"][0]["id"], "z-ai/glm-5.3-flash")
check("  配置本来就对 -> 不提示改地址", "suggestBaseURL" in res, False)

print()
print("=== 5. 每会话独立模型：执行中改设置不许打断 ===")


class FakeZC(object):
    def __init__(self):
        self.calls = []
        self.config_lock = asyncio.Lock()

    async def call(self, method, params=None, timeout=None):
        self.calls.append((method, params))
        return {}


app.zc = FakeZC()
app.scheduler = None
tm = app.TurnManager()

SID, M1, M2 = "sess_abcdefgh", {"providerId": "guanfang", "modelId": "glm-5.3-flash"}, \
    {"providerId": "zai", "modelId": "z-ai/glm-5.3-flash"}

# 会话在跑：记成待生效，且**绝不能**现在就去 setModel 打断它
tm.running[SID] = {"startedAt": 1}
tm.remember_pending(SID, model=M1)
check("执行中：改动被记下", tm.pending_of(SID)["model"], M1)
check("执行中：没有发出 setModel", len(app.zc.calls), 0)
check("执行中：desired 未被提前写脏（否则下一轮会被跳过）",
      (tm.desired.get(SID) or {}).get("model"), None)

# 下一轮发消息时落地
tm.running.pop(SID, None)
LOOP.run_until_complete(tm.apply_settings(SID, model=None))
check("下一轮：setModel 被发出", app.zc.calls[-1][0], "session/setModel")
check("下一轮：用的是待生效的模型", app.zc.calls[-1][1]["model"], M1)
check("下一轮：desired 已更新", tm.desired[SID]["model"], M1)
check("下一轮：pending 已清空", tm.pending_of(SID), {})

# 显式传入优先于 pending，且 pending 不该残留
tm.remember_pending(SID, model=M1)
LOOP.run_until_complete(tm.apply_settings(SID, model=M2))
check("显式传入优先", app.zc.calls[-1][1]["model"], M2)
check("pending 已消费", tm.pending_of(SID), {})

# 没有变化时不重复下发
n = len(app.zc.calls)
LOOP.run_until_complete(tm.apply_settings(SID, model=M2))
check("同值不重复下发", len(app.zc.calls), n)

print()
if FAIL:
    print("!!! %d 项失败：" % len(FAIL))
    for f in FAIL:
        print("   -", f)
    sys.exit(1)
print("全部通过 ✅")
