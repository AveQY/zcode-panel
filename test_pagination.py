# -*- coding: utf-8 -*-
"""本地校验历史消息分页（最近 50 条 + 上滑加载更早）的纯逻辑。

不连服务器：造一个临时 SQLite，按 zcode 的 message/part 表结构插数据，
验证 load_messages 的 limit/before 语义与 session_msg_stats 的统计。
"""
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import types

# ---- aiohttp 桩（同 test_running_guard.py）----
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


# ---- 造临时库 ----
tmpdir = tempfile.mkdtemp()
db = os.path.join(tmpdir, "db.sqlite")
conn = sqlite3.connect(db)
conn.execute("CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, "
             "data TEXT, sequence INTEGER, time_created INTEGER)")
conn.execute("CREATE TABLE part (message_id TEXT, data TEXT, sequence INTEGER)")
SID = "sess_test"
N = 120
for i in range(1, N + 1):
    role = "user" if i % 2 else "assistant"
    conn.execute("INSERT INTO message VALUES (?,?,?,?,?)",
                 ("m%04d" % i, SID, json.dumps({"role": role}), i, 1700000000000 + i))
    conn.execute("INSERT INTO part VALUES (?,?,?)",
                 ("m%04d" % i, json.dumps({"type": "text", "text": "msg-%d" % i}), 0))
conn.commit()
conn.close()
app.ZCODE_DB = db

# ---- 用例 ----
full = app.load_messages(SID)
check("全量加载条数", len(full), N)
check("全量加载顺序（首条 seq=1）", full[0]["seq"], 1)
check("消息带 seq 字段", full[5]["seq"], 6)

page1 = app.load_messages(SID, limit=50)
check("第一页条数=50", len(page1), 50)
check("第一页是最新的 50 条（首条 seq=71）", page1[0]["seq"], N - 49)
check("第一页仍是时间正序（末条 seq=120）", page1[-1]["seq"], N)

page2 = app.load_messages(SID, limit=50, before=page1[0]["seq"])
check("第二页条数=50", len(page2), 50)
check("第二页首条 seq=21", page2[0]["seq"], 21)
check("第二页末条 seq=70", page2[-1]["seq"], 70)

page3 = app.load_messages(SID, limit=50, before=page2[0]["seq"])
check("第三页条数=20（已到头）", len(page3), 20)
check("第三页首条 seq=1", page3[0]["seq"], 1)

total, mn = app.session_msg_stats(SID)
check("总数统计", total, N)
check("最小 sequence", mn, 1)
check("第一页 hasMore=True", page1[0]["seq"] > mn, True)
check("第三页 hasMore=False", page3[0]["seq"] > mn, False)

empty = app.load_messages("sess_none", limit=50)
check("空会话返回空列表", empty, [])
etotal, emn = app.session_msg_stats("sess_none")
check("空会话统计", (etotal, emn), (0, None))

# 边界：limit 超过总数
big = app.load_messages(SID, limit=500)
check("limit 大于总数时返回全量", len(big), N)

print()
print("PASS %d / FAIL %d" % (13 - len(FAIL), len(FAIL)))
sys.exit(1 if FAIL else 0)
