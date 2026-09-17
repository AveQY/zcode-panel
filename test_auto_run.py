# -*- coding: utf-8 -*-
"""本地校验「连续执行」的决策逻辑：收尾自动续跑 / 出错自动重试 / 停止总闸。

不连服务器：runner.send 用桩替换成记录调用；持久化打桩跳过。
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

# 跳过磁盘持久化（在实例化 runner 之前打桩）
app.load_auto_run = lambda runner: None
app.save_auto_run = lambda runner: None
# 加速：续跑延迟与重试退避都改成立即
app.AUTO_RUN_DELAY = 0.01
app.AUTO_RETRY_DELAYS = [0.01, 0.02, 0.03]

FAIL = []


def check(label, got, want):
    ok = got == want
    print("%-64s %s" % (label, "OK " if ok else "FAIL got=%r want=%r" % (got, want)))
    if not ok:
        FAIL.append(label)


class _FeedStub:
    """最小 feed 桩：只提供 stop_requested 与 broadcast 记录。"""

    def __init__(self, stop_requested=False):
        self.stop_requested = stop_requested
        self.broadcasts = []

    async def broadcast(self, payload):
        self.broadcasts.append(payload)


class _FeedMgrStub:
    def __init__(self, f=None):
        self.f = f

    def get(self, sid):
        return self.f


async def make_runner(feed_stub=None):
    app.feed = _FeedMgrStub(feed_stub) if feed_stub is not None else None
    r = app.TurnManager()
    sent = []

    async def fake_send(sid, content, mode=None, model=None, thought_level=None,
                        auto=False):
        sent.append((sid, content, auto))

    r.send = fake_send
    return r, sent


async def main():
    SID = "sess_autorun01"

    print("== 1. 未开启连续执行：收尾/出错都不动作 ==")
    r, sent = await make_runner()
    r._after_turn(SID, "completed")
    r._after_turn(SID, "failed")
    await asyncio.sleep(0.05)
    check("未开启时不发送", len(sent), 0)
    check("未开启时无状态", r.auto_of(SID).get("enabled"), None)

    print()
    print("== 2. 正常收尾 -> 自动续跑 ==")
    r2, sent2 = await make_runner()
    r2.set_auto(SID, True)
    check("开启后 enabled", r2.auto_of(SID).get("enabled"), True)
    check("默认续跑消息", r2.auto_of(SID).get("message"), "继续")
    r2.last_sent[SID] = "原始任务"
    r2._after_turn(SID, "completed")
    await asyncio.sleep(0.1)
    check("自动发出一条", len(sent2), 1)
    check("发出的是续跑消息", sent2[0][1], "继续")
    check("auto 标记为自动", sent2[0][2], True)
    check("轮数计数=1", r2.auto_of(SID).get("rounds"), 1)

    print()
    print("== 3. 出错 -> 重试同一条消息，3 次后关闭 ==")
    r3, sent3 = await make_runner()
    r3.set_auto(SID, True)
    r3.last_sent[SID] = "原始任务"
    r3._after_turn(SID, "failed")
    await asyncio.sleep(0.05)
    check("第 1 次重试发出原始消息", sent3 and sent3[0][1], "原始任务")
    check("重试计数=1", r3.auto_of(SID).get("retries"), 1)
    r3._after_turn(SID, "failed")
    await asyncio.sleep(0.05)
    check("重试计数=2", r3.auto_of(SID).get("retries"), 2)
    check("仍处于开启", r3.auto_of(SID).get("enabled"), True)
    r3._after_turn(SID, "failed")
    await asyncio.sleep(0.05)
    check("重试计数=3", r3.auto_of(SID).get("retries"), 3)
    r3._after_turn(SID, "failed")
    await asyncio.sleep(0.05)
    check("3 次后关闭", bool(r3.auto_of(SID).get("enabled")), False)
    n = len(sent3)
    r3._after_turn(SID, "failed")
    await asyncio.sleep(0.05)
    check("关闭后不再重试", len(sent3), n)

    print()
    print("== 4. 达到轮数上限后自动关闭 ==")
    r4, sent4 = await make_runner()
    r4.set_auto(SID, True, max_rounds=2)
    for i in range(3):
        r4._after_turn(SID, "completed")
        await asyncio.sleep(0.05)
    check("第 1 轮发出", len(sent4) >= 1, True)
    check("上限 2 轮 -> 第 3 次收尾后关闭", bool(r4.auto_of(SID).get("enabled")), False)

    print()
    print("== 5. 手动停止（cancel）是总闸 ==")
    r5, sent5 = await make_runner()
    r5.set_auto(SID, True)
    r5._after_turn(SID, "cancelled")
    check("cancelled 后关闭", bool(r5.auto_of(SID).get("enabled")), False)
    await asyncio.sleep(0.05)
    check("cancelled 不发送", len(sent5), 0)

    print()
    print("== 6. 用户点过停止（stop_requested）不再续跑 ==")
    f6 = _FeedStub(stop_requested=True)
    r6, sent6 = await make_runner(f6)
    r6.set_auto(SID, True)
    r6._after_turn(SID, "completed")
    await asyncio.sleep(0.05)
    check("stop_requested 时不动", len(sent6), 0)
    check("状态保留（等下一轮人工确认）", r6.auto_of(SID).get("enabled"), True)

    print()
    print("== 7. 手动发消息重置连续计数 ==")
    r7, _ = await make_runner()
    r7.set_auto(SID, True)
    r7._after_turn(SID, "completed")
    r7._after_turn(SID, "completed")
    await asyncio.sleep(0.05)
    check("已累计 2 轮", r7.auto_of(SID).get("rounds"), 2)
    r7.note_user_send(SID, "新的指令")
    check("手动发消息后轮数清零", r7.auto_of(SID).get("rounds"), 0)
    check("手动发消息后重试清零", r7.auto_of(SID).get("retries"), 0)

    print()
    print("== 8. 自动发送撞上手动操作 -> 让位不报错 ==")
    r8, sent8 = await make_runner()
    r8.set_auto(SID, True)
    r8.last_sent[SID] = "原始任务"

    async def busy_send(sid, content, mode=None, model=None, thought_level=None,
                        auto=False):
        raise RuntimeError("这个会话正在执行中，请稍候")

    r8.send = busy_send
    r8._after_turn(SID, "completed")
    await asyncio.sleep(0.1)
    check("连续执行仍开启（让位不关闭）", r8.auto_of(SID).get("enabled"), True)

    print()
    print("== 9. 正常收尾清零重试计数 ==")
    r9, _ = await make_runner()
    r9.set_auto(SID, True)
    r9.auto_of(SID)["retries"] = 2
    r9._after_turn(SID, "completed")
    check("completed 后 retries 清零", r9.auto_of(SID).get("retries"), 0)


if __name__ == "__main__":
    asyncio.run(main())
    print()
    if FAIL:
        print("FAIL %d 项:" % len(FAIL))
        for f in FAIL:
            print("  -", f)
        sys.exit(1)
    print("全部通过")
