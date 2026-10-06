"""交接协议规则测试（零第三方依赖）。"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import unittest
from collections import Counter

from app.store import (
    ACCEPTED,
    BAD_REQUEST,
    CONFLICT,
    FORBIDDEN,
    OK,
    Store,
)


class StoreCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "handoff.db")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def store(self, count: int = 6) -> Store:
        return Store(self.path, partition_count=count)

    # ---------------------------------------------------------- 基本交接流程

    def test_initial_snapshot_assigns_every_partition_once(self) -> None:
        s = self.store()
        code, body = s.snapshot("r1", ["a", "b"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "stable")
        view = s.assignments_view()
        self.assertEqual(set(view["assignments"]), {str(i) for i in range(6)})
        owners = Counter(v["owner"] for v in view["assignments"].values())
        # 稳定哈希：i % 2
        self.assertEqual(owners, Counter({"a": 3, "b": 3}))
        for part, info in view["assignments"].items():
            self.assertEqual(info["owner"], "b" if int(part) % 2 else "a")

    def test_member_replacement_enters_revocation_without_double_ownership(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        code, body = s.snapshot("r2", ["b", "c"])
        self.assertEqual(code, ACCEPTED)
        self.assertEqual(body["status"], "revoking")
        self.assertEqual(
            sorted(body["revocations"], key=lambda x: x["part"]),
            [
                {"part": "0", "owner": "a", "target": "b"},
                {"part": "1", "owner": "b", "target": "c"},
                {"part": "2", "owner": "a", "target": "b"},
                {"part": "3", "owner": "b", "target": "c"},
                {"part": "4", "owner": "a", "target": "b"},
                {"part": "5", "owner": "b", "target": "c"},
            ],
        )
        # 撤销期间读模型只能看到旧完整分配
        view = s.assignments_view()
        self.assertEqual(view["epoch"], 0)
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "a", "1": "b", "2": "a", "3": "b", "4": "a", "5": "b"},
        )
        for p in ("0", "2", "4"):
            self.assertEqual(view["assignments"][p]["revoking"], "b")
        self._assert_unique_ownership(view)

    def test_revoked_partition_is_not_handed_to_new_instance_before_confirm(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        # 新实例 c 此刻看不到任何属于自己的分区
        c_view = s.assignments_view(member="c")
        self.assertEqual(c_view["assignments"], {})

    def test_confirm_deletes_old_owner_advances_and_publishes_atomically(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        code, body = s.confirm("c1", "a", ["4", "0", "2"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "partially_released")
        self.assertEqual(body["epoch"], 1)
        view = s.assignments_view()
        self.assertEqual(view["epoch"], 1)
        # 已确认分区交给新实例，未确认分区仍是旧完整分配
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "b", "1": "b", "2": "b", "3": "b", "4": "b", "5": "b"},
        )
        for p in ("0", "2", "4"):
            self.assertEqual(view["assignments"][p]["epoch"], 1)
        for p in ("1", "3", "5"):
            self.assertEqual(view["assignments"][p]["revoking"], "c")
        self._assert_unique_ownership(view)

        code, body = s.confirm("c2", "b", ["5", "1", "3"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "completed")
        self.assertEqual(body["remaining"], [])
        view = s.assignments_view()
        self.assertEqual(view["epoch"], 2)
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "b", "1": "c", "2": "b", "3": "c", "4": "b", "5": "c"},
        )
        self.assertFalse(s.handover_view()["active"])
        self._assert_unique_ownership(view)

    def test_removed_member_partitions_are_revoked_to_new_targets(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        code, body = s.snapshot("r2", ["c"])
        self.assertEqual(code, ACCEPTED)
        self.assertEqual({r["target"] for r in body["revocations"]}, {"c"})
        code, _ = s.confirm("c1", "a", ["0", "2", "4"])
        self.assertEqual(code, OK)
        code, _ = s.confirm("c2", "b", ["1", "3", "5"])
        self.assertEqual(code, OK)
        view = s.assignments_view()
        self.assertEqual({v["owner"] for v in view["assignments"].values()}, {"c"})

    # ---------------------------------------------------------- 失效确认

    def test_confirmation_without_active_handover_is_expired(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a"])
        code, body = s.confirm("x1", "a", ["0"])
        self.assertEqual(code, CONFLICT)
        self.assertEqual(body["error"], "confirmation_expired")
        # 拒绝确认不得推进代次
        self.assertEqual(s.assignments_view()["epoch"], 0)

    def test_foreign_owner_confirmation_is_forbidden_and_not_advanced(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        code, body = s.confirm("x2", "c", ["0"])  # 0 仍属于 a
        self.assertEqual(code, FORBIDDEN)
        self.assertEqual(body["error"], "not_owner")
        self.assertEqual(s.assignments_view()["assignments"]["0"]["owner"], "a")
        self.assertEqual(s.assignments_view()["epoch"], 0)

    def test_confirmation_with_unexpected_partitions_is_rejected(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        # 先合法确认一部分
        self.assertEqual(s.confirm("c1", "a", ["0", "2"])[0], OK)
        # 含已释放的旧分区（多余）
        code, body = s.confirm("x3", "a", ["0", "4"])
        self.assertEqual(code, BAD_REQUEST)
        self.assertEqual(body["error"], "unexpected_partitions")
        # 整笔拒绝：4 也没有被推进
        self.assertEqual(s.assignments_view()["assignments"]["4"]["owner"], "a")
        # 完全不在撤销集合内
        code, body = s.confirm("x4", "b", ["7"])
        self.assertEqual(code, BAD_REQUEST)
        self.assertEqual(body["partitions"], ["7"])

    def test_duplicate_part_or_empty_lists_are_rejected(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        with self.assertRaises(Exception):
            s.confirm("x5", "a", ["0", "0"])
        with self.assertRaises(Exception):
            s.confirm("x6", "a", [])

    # ---------------------------------------------------------- 幂等与冲突

    def test_same_request_replays_first_result(self) -> None:
        s = self.store()
        c1, b1 = s.snapshot("r1", ["a", "b"])
        c2, b2 = s.snapshot("r1", ["a", "b"])
        self.assertEqual((c1, b1), (c2, b2))

        s.snapshot("r2", ["b", "c"])
        c3, b3 = s.confirm("c1", "a", ["0", "2", "4"])
        c4, b4 = s.confirm("c1", "a", ["0", "2", "4"])
        self.assertEqual((c3, b3), (c4, b4))
        # 重放不得再次推进代次
        self.assertEqual(s.assignments_view()["epoch"], 1)

    def test_same_request_id_with_different_snapshot_conflicts(self) -> None:
        s = self.store()
        self.assertEqual(s.snapshot("r1", ["a", "b"])[0], OK)
        code, body = s.snapshot("r1", ["a", "c"])
        self.assertEqual(code, CONFLICT)
        self.assertEqual(body["error"], "idempotency_conflict")

    def test_request_id_reused_across_kinds_conflicts(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        self.assertEqual(s.confirm("r1", "a", ["0"])[0], CONFLICT)

    def test_failed_confirmation_id_is_still_replayable(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        first = s.confirm("x1", "c", ["0"])  # 越权
        replay = s.confirm("x1", "c", ["0"])
        self.assertEqual(first, replay)

    # -------------------------------------------------- 进行中快照重新收敛

    def test_snapshot_during_handover_is_accepted_and_queued(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        # 交接进行中：后续完整快照被受理排队（202 queued），并回带持久化目标
        code, body = s.snapshot("r3", ["x", "y"])
        self.assertEqual(code, ACCEPTED)
        self.assertEqual(body["status"], "queued")
        self.assertEqual(body["request_id"], "r3")
        self.assertEqual(body["after_handover"], "r2")
        self.assertEqual(
            body["target"],
            {"0": "b", "1": "c", "2": "b", "3": "c", "4": "b", "5": "c"},
        )
        # 排队快照已持久化，交接视图可见
        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["queued_snapshot"]["request_id"], "r3")
        self.assertEqual(view["queued_snapshot"]["members"], ["x", "y"])

        # 相同标识 + 相同成员快照：稳定重放首次 queued 结果
        code2, body2 = s.snapshot("r3", ["y", "x"])  # 排序后等价
        self.assertEqual((code2, body2), (code, body))

        # 复用标识但成员不同：明确冲突
        code, body = s.snapshot("r3", ["x", "z"])
        self.assertEqual(code, CONFLICT)
        self.assertEqual(body["error"], "idempotency_conflict")

        # 已有另一份等待快照：新的不同标识快照明确冲突
        code, body = s.snapshot("r4", ["p", "q"])
        self.assertEqual(code, CONFLICT)
        self.assertEqual(body["error"], "deferred_snapshot_pending")

        # 完成当前交接后，排队快照基于持久化归属形成下一轮交接
        s.confirm("c1", "a", ["0", "2", "4"])
        code, body = s.confirm("c2", "b", ["1", "3", "5"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "completed")
        self.assertEqual(body["next_handover"]["request_id"], "r3")
        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r3")
        self.assertEqual(view["snapshot"], ["x", "y"])
        self.assertEqual(
            {r["part"]: r["target"] for r in view["revocations"]},
            {str(i): ("y" if i % 2 else "x") for i in range(6)},
        )
        self._assert_unique_ownership(s.assignments_view())

    def test_converging_snapshot_after_noop_is_stable(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.confirm("c1", "a", ["0", "2", "4"])
        s.confirm("c2", "b", ["1", "3", "5"])
        code, body = s.snapshot("r5", ["c", "b"])  # 排序后与 ["b","c"] 同
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "stable")

    # ------------------------------------------------- 受理后续快照并连续交接

    def test_three_snapshots_chained_handover_converges_across_restart(self) -> None:
        """验收主场景：三次连续成员快照。

        r1 建立初始分配；r2 触发撤销；r3 在撤销期间被受理排队。
        完成 r2 的全部合法确认后，r3 成员集合必须开始其应有交接；
        重启后结论保持一致，无双重归属、无丢失、无旧确认推进新一轮。
        """
        s = self.store()
        self.assertEqual(s.snapshot("r1", ["a", "b"])[0], OK)
        code, body = s.snapshot("r2", ["b", "c"])  # 第二次：触发撤销
        self.assertEqual(code, ACCEPTED)
        self.assertEqual(body["status"], "revoking")
        code, body = s.snapshot("r3", ["c", "d"])  # 第三次：撤销期间受理
        self.assertEqual(code, ACCEPTED)
        self.assertEqual(body["status"], "queued")

        # r3 成员集合此刻还不能拿到任何分区（撤销中的分区仍归旧持有者）
        self.assertEqual(s.assignments_view(member="d")["assignments"], {})

        # 完成 r2 的全部合法确认
        code, body = s.confirm("c1", "a", ["0", "2", "4"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "partially_released")
        self.assertNotIn("next_handover", body)  # 还有 b 的分区未释放
        code, body = s.confirm("c2", "b", ["1", "3", "5"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "completed")

        # 最后一次确认已在同一事务内开启 r3 的下一轮交接
        nxt = body["next_handover"]
        self.assertEqual(nxt["request_id"], "r3")
        self.assertEqual(nxt["status"], "revoking")

        def assert_round3_started(store: Store) -> None:
            view = store.handover_view()
            self.assertTrue(view["active"], "r3 交接应已开始")
            self.assertEqual(view["request_id"], "r3")
            self.assertEqual(view["snapshot"], ["c", "d"])
            # 排队快照已消费，不再滞留于交接视图
            self.assertNotIn("queued_snapshot", view)
            # r2 完成后：b 持偶数、c 持奇数；r3=[c,d] 目标 偶->c 奇->d。
            # 因而全部 6 个分区都要由当时持有者释放：偶 b->c，奇 c->d。
            self.assertEqual(
                {r["part"]: (r["owner"], r["target"]) for r in view["revocations"]},
                {
                    "0": ("b", "c"), "1": ("c", "d"),
                    "2": ("b", "c"), "3": ("c", "d"),
                    "4": ("b", "c"), "5": ("c", "d"),
                },
            )
            self.assertEqual(view["released"], [])
            # 需要撤销的分区仍只能由当时持有者释放
            self.assertEqual(
                {r["part"]: r["owner"] for r in view["revocations"]},
                {"0": "b", "1": "c", "2": "b", "3": "c", "4": "b", "5": "c"},
            )

        assert_round3_started(s)
        self._assert_unique_ownership(s.assignments_view())

        # 旧交接/越权确认不能推进新一轮：
        # 0 在 r3 中只由持有者 b 释放，c 越权拒绝
        code, body = s.confirm("old1", "c", ["0"])
        self.assertEqual(code, FORBIDDEN)
        # a 已不持有任何分区
        code, body = s.confirm("old2", "a", ["0"])
        self.assertEqual(code, FORBIDDEN)
        # 多余/不存在于撤销集合的分区整笔拒绝，不推进
        code, body = s.confirm("old3", "b", ["0", "9"])
        self.assertEqual(code, BAD_REQUEST)

        # 重启：已受理快照不丢失，r3 交接结论保持
        s.close()
        s2 = self.store()
        assert_round3_started(s2)
        self.assertEqual(s2.assignments_view()["epoch"], 2)

        # 完成 r3：偶数由 b 释放给 c，奇数由 c 释放给 d
        code, body = s2.confirm("c3", "b", ["0", "2", "4"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "partially_released")
        code, body = s2.confirm("c4", "c", ["1", "3", "5"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "completed")
        self.assertNotIn("next_handover", body)
        view = s2.assignments_view()
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "c", "1": "d", "2": "c", "3": "d", "4": "c", "5": "d"},
        )
        self.assertFalse(s2.handover_view()["active"])
        self._assert_unique_ownership(view)

        # 再次重启，最终分配稳定不变
        s2.close()
        s3 = self.store()
        view = s3.assignments_view()
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "c", "1": "d", "2": "c", "3": "d", "4": "c", "5": "d"},
        )
        self.assertFalse(s3.handover_view()["active"])
        self._assert_unique_ownership(view)

    def test_queued_snapshot_that_converges_completes_without_revocation(self) -> None:
        """排队快照若与完成后的归属一致，应直接稳定，不产生新一轮撤销。"""
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        # r3 与 r2 同成员：r2 完成后当前归属已与之一致
        code, _ = s.snapshot("r3", ["b", "c"])
        self.assertEqual(code, ACCEPTED)
        s.confirm("c1", "a", ["0", "2", "4"])
        code, body = s.confirm("c2", "b", ["1", "3", "5"])
        self.assertEqual(body["next_handover"]["request_id"], "r3")
        self.assertEqual(body["next_handover"]["status"], "stable")
        self.assertEqual(body["next_handover"]["revocations"], [])
        self.assertFalse(s.handover_view()["active"])
        self.assertNotIn("queued_snapshot", s.handover_view())
        self.assertEqual(
            {p: v["owner"] for p, v in s.assignments_view()["assignments"].items()},
            {"0": "b", "1": "c", "2": "b", "3": "c", "4": "b", "5": "c"},
        )

    def test_queued_snapshot_survives_crash_before_promotion_commit(self) -> None:
        """最后确认提交前崩溃：确认与推进整体回滚，受理快照仍在排队。"""
        import subprocess
        import sys

        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.confirm("c1", "a", ["0", "2", "4"])
        s.snapshot("r3", ["c", "d"])
        s.close()

        script = (
            "import os, sys; sys.path.insert(0, %r);"
            " from app.store import Store;"
            " s = Store(%r, partition_count=6);"
            " s.confirm('c2', 'b', ['1','3','5'], _crash_hook=lambda: os._exit(42))"
            % (
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                self.path,
            )
        )
        proc = subprocess.run([sys.executable, "-c", script], capture_output=True)
        self.assertEqual(proc.returncode, 42, proc.stderr.decode())

        s2 = self.store()
        # r2 仍在进行中，r3 仍排队，未丢失
        view = s2.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r2")
        self.assertEqual(view["queued_snapshot"]["request_id"], "r3")
        # 重传 c2 正常完成并推进 r3
        code, body = s2.confirm("c2", "b", ["1", "3", "5"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "completed")
        self.assertEqual(body["next_handover"]["request_id"], "r3")
        self.assertEqual(s2.handover_view()["request_id"], "r3")
        self._assert_unique_ownership(s2.assignments_view())

    # ---------------------------------------------------------- 并发不变量

    def test_concurrent_confirms_keep_single_owner_and_replay(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        errors: list[BaseException] = []

        def fire(req_id: str, member: str, parts: list[str]) -> None:
            try:
                c1, _ = s.confirm(req_id, member, parts)
                c2, b2 = s.confirm(req_id, member, parts)
                self.assertEqual(c1, c2)
                self.assertIn(b2["status"], ("partially_released", "completed"))
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=fire, args=("c1", "a", ["0", "2", "4"]))
        t2 = threading.Thread(target=fire, args=("c2", "b", ["1", "3", "5"]))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(errors, [])
        view = s.assignments_view()
        self._assert_unique_ownership(view)
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "b", "1": "c", "2": "b", "3": "c", "4": "b", "5": "c"},
        )

    def test_concurrent_snapshots_cannot_open_two_handovers(self) -> None:
        s = self.store()
        s.snapshot("r0", ["a"])
        results: list[tuple[int, str]] = []

        def fire(req_id: str) -> None:
            code, body = s.snapshot(req_id, ["z"])
            results.append((code, body.get("error", body.get("status", ""))))

        threads = [
            threading.Thread(target=fire, args=(f"r{i}",)) for i in range(1, 16)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 恰好一个开启撤销交接；并发进行中至多一个被受理排队，其余明确冲突。
        revoking = [r for r in results if r[1] == "revoking"]
        queued = [r for r in results if r[1] == "queued"]
        self.assertEqual(len(revoking), 1)
        self.assertLessEqual(len(queued), 1)
        self.assertTrue(
            all(
                r[1] in ("revoking", "queued", "deferred_snapshot_pending")
                for r in results
            )
        )
        with sqlite3.connect(self.path) as db:
            # 只有进行中交接的撤销；排队快照尚未产生第二批撤销
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM revocations").fetchone()[0], 6
            )
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM handover_state WHERE status='pending'")
                .fetchone()[0],
                1,
            )
        self._assert_unique_ownership(s.assignments_view())

    def test_observable_states_are_always_consistent(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        stop = threading.Event()
        bad: list[dict] = []

        def reader() -> None:
            while not stop.is_set():
                view = s.assignments_view()
                owners = [v["owner"] for v in view["assignments"].values()]
                if len(owners) != len(set(view["assignments"])):
                    bad.append(view)
                # 已公布到新代次的分区不应再挂在旧实例名下出现两次
                seen: set[str] = set()
                for part, info in view["assignments"].items():
                    if part in seen:
                        bad.append(view)
                    seen.add(part)

        th = threading.Thread(target=reader)
        th.start()
        s.confirm("c1", "a", ["0", "2", "4"])
        s.confirm("c2", "b", ["1", "3", "5"])
        stop.set()
        th.join()
        self.assertEqual(bad, [])

    def test_concurrent_confirms_across_separate_connections(self) -> None:
        """两个独立连接（模拟多进程）并发确认，不得报错或双重归属。"""
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.close()
        s1 = self.store()
        s2 = self.store()
        errors: list[BaseException] = []

        def fire(store: Store, req_id: str, member: str, parts: list[str]) -> None:
            try:
                code, _ = store.confirm(req_id, member, parts)
                self.assertEqual(code, OK)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=fire, args=(s1, "c1", "a", ["0", "2", "4"]))
        t2 = threading.Thread(target=fire, args=(s2, "c2", "b", ["1", "3", "5"]))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(errors, [])
        view = s1.assignments_view()
        self.assertEqual(view["epoch"], 2)
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "b", "1": "c", "2": "b", "3": "c", "4": "b", "5": "c"},
        )
        self.assertFalse(s1.handover_view()["active"])

    def test_concurrent_final_confirms_promote_queued_snapshot_once(self) -> None:
        """跨连接并发完成最后一批确认：排队快照只被推进一次，无双重归属。"""
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        self.assertEqual(s.snapshot("r3", ["c", "d"])[0], ACCEPTED)
        s.close()
        s1 = self.store()
        s2 = self.store()
        errors: list[BaseException] = []

        def fire(store: Store, req_id: str, member: str, parts: list[str]) -> None:
            try:
                code, _ = store.confirm(req_id, member, parts)
                self.assertEqual(code, OK)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        # 两个持有者的最终确认跨连接并发；恰好一个看到 remaining==0 并推进 r3
        t1 = threading.Thread(target=fire, args=(s1, "c1", "a", ["0", "2", "4"]))
        t2 = threading.Thread(target=fire, args=(s2, "c2", "b", ["1", "3", "5"]))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(errors, [])

        view = s1.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r3")
        self.assertNotIn("queued_snapshot", view)
        self.assertEqual(
            {r["part"]: (r["owner"], r["target"]) for r in view["revocations"]},
            {
                "0": ("b", "c"), "1": ("c", "d"),
                "2": ("b", "c"), "3": ("c", "d"),
                "4": ("b", "c"), "5": ("c", "d"),
            },
        )
        self._assert_unique_ownership(s1.assignments_view())
        # 排队快照只被消费一次：deferred 表为空，撤销集合恰好 6 个
        import sqlite3 as _sql

        with _sql.connect(self.path) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM deferred_snapshot").fetchone()[0], 0
            )
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM revocations").fetchone()[0], 6
            )

    def test_handover_view_tracks_persisted_target_and_released_set(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r2")
        self.assertEqual(view["snapshot"], ["b", "c"])
        self.assertEqual(
            view["target"],
            {"0": "b", "1": "c", "2": "b", "3": "c", "4": "b", "5": "c"},
        )
        self.assertEqual(view["released"], [])
        s.confirm("c1", "a", ["0", "2", "4"])
        view = s.handover_view()
        self.assertEqual(view["released"], ["0", "2", "4"])
        self.assertEqual(
            {r["part"]: r["target"] for r in view["revocations"]},
            {"1": "c", "3": "c", "5": "c"},
        )

    # ---------------------------------------------------------- 持久化恢复

    def test_state_survives_reopen(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.confirm("c1", "a", ["0", "2", "4"])
        s.close()
        s2 = self.store()
        view = s2.assignments_view()
        self.assertEqual(view["epoch"], 1)
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "b", "1": "b", "2": "b", "3": "b", "4": "b", "5": "b"},
        )
        # 已提交的确认请求重放首次结果
        code, body = s2.confirm("c1", "a", ["0", "2", "4"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "partially_released")

    def test_crash_between_release_and_publish_leaves_old_assignment(self) -> None:
        """模拟进程在释放语句执行后、提交/发布前被杀死。"""
        import subprocess
        import sys

        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.close()

        script = (
            "import os, sys; sys.path.insert(0, %r);"
            " from app.store import Store;"
            " s = Store(%r, partition_count=6);"
            " s.confirm('c1', 'a', ['0','2','4'], _crash_hook=lambda: os._exit(42))"
            % (os.path.dirname(os.path.dirname(os.path.abspath(__file__))), self.path)
        )
        proc = subprocess.run(
            [sys.executable, "-c", script], capture_output=True
        )
        self.assertEqual(proc.returncode, 42, proc.stderr.decode())

        # 重启：事务整体未提交，旧完整分配原封不动
        s2 = self.store()
        view = s2.assignments_view()
        self.assertEqual(view["epoch"], 0)
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "a", "1": "b", "2": "a", "3": "b", "4": "a", "5": "b"},
        )
        # 请求标识未被占用，重传后正常完成，不产生双重归属
        code, body = s2.confirm("c1", "a", ["0", "2", "4"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "partially_released")
        self.assertEqual(view["epoch"], 0)
        self.assertEqual(s2.assignments_view()["epoch"], 1)
        self._assert_unique_ownership(s2.assignments_view())

    def test_crash_after_commit_replays_first_result_on_restart(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.confirm("c1", "a", ["0", "2", "4"])
        s.close()
        s2 = self.store()  # 进程重启
        code, body = s2.confirm("c1", "a", ["0", "2", "4"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "partially_released")
        self.assertEqual(s2.assignments_view()["epoch"], 1)

    # ------------------------------------------------------------------ utils

    @staticmethod
    def _assert_unique_ownership(view: dict) -> None:
        # assignments 以分区为主键，结构上保证；此处再显式校验读模型
        parts = list(view["assignments"])
        assert len(parts) == len(set(parts))
        for info in view["assignments"].values():
            assert info["owner"]


if __name__ == "__main__":
    unittest.main()
