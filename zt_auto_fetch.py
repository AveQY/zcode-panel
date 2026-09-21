#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""zt_auto_fetch.py —— 禅道需求巡检定时触发器（cron 每 10 分钟 + 空闲补发）

行为：
  - 正常模式（cron */10）：登录面板 → 找 /root/miniphone 会话 →
      idle  → 发送巡检提示，并清除 pending 标记
      busy  → 跳过，并写下 pending 标记（等空闲补发）
  - 补发模式（--pending-only，cron 每分钟）：仅当存在 pending 标记且会话已空闲时发送。
      这样「正在执行时跳过、一空闲立刻补上」，最多延迟 1 分钟。
  - 发送完全跟随会话自身设置（2026-09-21）：模型显式取「排队改动 > 会话当前模型」
      下发给面板（漂移会被纠正回来）；模式与思考等级不传——排队改动照常生效，
      否则会话保持自己的设置。不再写死 mode=build。

用法：
  zt_auto_fetch.py                # 每 10 分钟的正常巡检
  zt_auto_fetch.py --pending-only # 每分钟的空闲补发探测
  zt_auto_fetch.py --check        # 只检查登录/会话状态，不发消息
  zt_auto_fetch.py --rebind       # 丢弃旧绑定，重新绑定工作区第一个会话

会话绑定：
  首次运行会把目标会话写进 /opt/zcode-chat/data/zt_fetch_target.json，
  之后只认这个会话——**它被删除后巡检自动空转**（面板删除会话时会联动
  解绑并摘掉 crontab 巡检行）。换目标：--rebind 或删掉绑定文件。
"""
import asyncio
import json
import os
import sys
import time

import aiohttp

BASE = "http://127.0.0.1:8081"
WORKSPACE = "/root/miniphone"
CONFIG = "/opt/zcode-chat/config.json"
TARGET_FILE = "/opt/zcode-chat/data/zt_fetch_target.json"
PENDING = "/tmp/zt_auto_fetch.pending"
PENDING_TTL = 2400          # pending 标记有效期 40 分钟，超时作废

PROMPT = (
    "【定时触发·禅道需求巡检】请使用最新的 zentao / zentao-agent skill 完成：\n"
    "1) 调用 skill 的脚本（zt.sh / pipeline.sh 等）获取禅道上最新的需求与任务列表；\n"
    "2) 如有新需求或待处理任务：按 skill 流程继续开发实现，完成后更新禅道状态"
    "（状态流转 + 填写备注），并把代码 git 提交推送到远端仓库；\n"
    "3) 如无新任务：直接简短回复「无新需求」即可，不要做任何改动。\n"
    "（本条由服务器 cron 自动触发，请勿重复设置定时任务。）"
)


def login_key():
    with open(CONFIG) as fh:
        cfg = json.load(fh)
    return cfg.get("key_plain") or "admin123"


def write_pending():
    try:
        with open(PENDING, "w") as fh:
            fh.write(str(time.time()))
    except OSError:
        pass


def read_pending():
    try:
        ts = float(open(PENDING).read().strip())
    except (OSError, ValueError):
        return None
    if time.time() - ts > PENDING_TTL:
        try:
            os.remove(PENDING)
        except OSError:
            pass
        return None
    return ts


def clear_pending():
    try:
        os.remove(PENDING)
    except OSError:
        pass


def _valid_model(cand):
    return isinstance(cand, dict) and bool(cand.get("providerId")) and bool(cand.get("modelId"))


def resolve_send_model(target):
    """跟随会话自身的模型设置：排队中的改动（执行期间改的）优先，其次当前模型。

    显式把模型随消息下发给面板，定时轮次永远落在「该会话配置的模型」上；
    会话模型若被运行时兜底改漂移过，也会被这里纠正回来。
    """
    for cand in (target.get("pendingModel"), target.get("model")):
        if _valid_model(cand):
            return {"providerId": cand["providerId"], "modelId": cand["modelId"]}
    return None


async def main():
    pending_only = "--pending-only" in sys.argv
    check_only = "--check" in sys.argv
    async with aiohttp.ClientSession() as http:
        async with http.post(BASE + "/api/login", json={"key": login_key()}) as r:
            data = await r.json()
        tok = data.get("token")
        if not data.get("ok") or not tok:
            print("[fail] 登录失败:", data)
            return 1
        headers = {"Cookie": "zwc_token=" + tok}

        async with http.get(BASE + "/api/sessions", headers=headers) as r:
            sess = await r.json()
        sessions = sess.get("sessions", [])
        rebind = "--rebind" in sys.argv
        bound = None
        if not rebind:
            try:
                with open(TARGET_FILE) as fh:
                    bound = (json.load(fh) or {}).get("sessionId")
            except (OSError, ValueError):
                bound = None
        if bound:
            # 严格按绑定走：绑定的会话没了就空转，绝不自动换目标
            target = next((s for s in sessions if s.get("id") == bound), None)
            if not target:
                print("[stop] 绑定的会话 %s 不存在（可能已删除），巡检空转；"
                      "要换目标执行 zt_auto_fetch.py --rebind" % bound)
                return 0
        else:
            target = next((s for s in sessions
                           if s.get("workspacePath") == WORKSPACE
                           and not s.get("archived")), None)
            if not target:
                print("[fail] 未找到工作区", WORKSPACE, "的会话")
                return 1
            try:
                with open(TARGET_FILE, "w") as fh:
                    json.dump({"sessionId": target["id"],
                               "workspace": WORKSPACE,
                               "boundAt": time.strftime("%Y-%m-%d %H:%M:%S")},
                              fh, ensure_ascii=False)
                print("[bind] 巡检已绑定会话", target["id"])
            except OSError:
                pass
        sid = target["id"]
        status = target.get("status")
        print("[info] 会话:", sid, "状态:", status)
        follow_model = resolve_send_model(target)
        if follow_model:
            model_desc = "%s/%s" % (follow_model["providerId"], follow_model["modelId"])
            if _valid_model(target.get("pendingModel")):
                model_desc += "（排队改动优先落地）"
        else:
            model_desc = "会话尚未设置模型，跟随其当前模型"
        print("[info] 跟随会话设置: mode=%s（不强加） model=%s"
              % (target.get("mode") or "-", model_desc))
        if check_only:
            print("[check] OK（未发送消息）")
            return 0

        if status != "idle":
            if not pending_only:
                write_pending()
                print("[skip] 会话忙（%s），已登记待空闲补发" % status)
            return 0

        # 空闲：补发模式没有 pending 标记就不动作
        if pending_only and read_pending() is None:
            return 0
        clear_pending()

        async with http.ws_connect(BASE + "/ws", headers=headers) as ws:
            # mode/thoughtLevel 传 None：面板先落地「排队改动」，否则会话保持自己的设置；
            # model 显式传「排队改动 > 当前模型」，见 resolve_send_model。
            frame = {"type": "send", "sessionId": sid, "content": PROMPT,
                     "mode": None, "model": follow_model, "thoughtLevel": None}
            await ws.send_str(json.dumps(frame, ensure_ascii=False))
            acked = False
            try:
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        break
                    ev = json.loads(msg.data)
                    t = ev.get("type")
                    if t in ("accepted", "started", "resumed"):
                        print("[sent] %s -> %s（%s，model=%s）"
                              % (t, sid, "空闲补发" if pending_only else "定时巡检",
                                 "%s/%s" % (follow_model["providerId"], follow_model["modelId"])
                                 if follow_model else "跟随会话"))
                        acked = True
                        break
                    if t == "error":
                        print("[fail] 面板报错:", ev)
                        break
            except asyncio.TimeoutError:
                print("[warn] 未收到确认帧（消息可能已送达）")
            return 0 if acked else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()) or 0)
