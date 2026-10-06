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

    # -------------------------------------------------- 进行中快照排队与串联

    def test_snapshot_during_handover_is_queued_with_persisted_target(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        code, body = s.snapshot("r3", ["c", "d"])
        self.assertEqual(code, ACCEPTED)
        self.assertEqual(body["status"], "queued")
        # 响应携带已持久化目标，调用方可据此重新收敛
        self.assertEqual(body["after_handover"], "r2")
        self.assertEqual(
            body["target"],
            {"0": "b", "1": "c", "2": "b", "3": "c", "4": "b", "5": "c"},
        )
        # 排队期间第三轮成员尚不可见任何分区
        self.assertEqual(s.assignments_view(member="d")["assignments"], {})
        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r2")
        self.assertEqual(view["queued_snapshot"]["request_id"], "r3")
        # 相同标识 + 相同成员快照稳定重放 queued
        code2, body2 = s.snapshot("r3", ["c", "d"])
        self.assertEqual((code, body), (code2, body2))

    def test_queued_request_id_with_different_members_conflicts(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        self.assertEqual(s.snapshot("r3", ["c", "d"])[0], ACCEPTED)
        code, body = s.snapshot("r3", ["c", "e"])
        self.assertEqual(code, CONFLICT)
        self.assertEqual(body["error"], "idempotency_conflict")
        # 排队槽仍是原快照
        self.assertEqual(
            s.handover_view()["queued_snapshot"]["members"], ["c", "d"]
        )

    def test_second_waiting_snapshot_conflicts(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        self.assertEqual(s.snapshot("r3", ["c", "d"])[0], ACCEPTED)
        code, body = s.snapshot("r4", ["e", "f"])
        self.assertEqual(code, CONFLICT)
        self.assertEqual(body["error"], "deferred_snapshot_pending")
        self.assertEqual(body["queued_request_id"], "r3")

    def test_queued_snapshot_request_id_cannot_be_used_for_confirm(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        self.assertEqual(s.snapshot("r3", ["c", "d"])[0], ACCEPTED)
        # 即便确认本身合法，标识已被排队快照占用，必须明确冲突
        code, body = s.confirm("r3", "a", ["0", "2", "4"])
        self.assertEqual(code, CONFLICT)
        self.assertEqual(body["error"], "idempotency_conflict")
        self.assertEqual(s.assignments_view()["epoch"], 0)

    def test_chained_snapshots_form_next_handover_on_last_confirmation(self) -> None:
        """三次连续快照：第二次触发撤销，第三次撤销期间被受理（queued）。

        第二次的全部合法确认完成后，第三次成员集合必须已开始其应有交接；
        需要撤销的分区仍只能由当时持有者释放，旧确认不能推进新一轮。
        """
        s = self.store()
        self.assertEqual(s.snapshot("r1", ["a", "b"])[0], OK)
        code, body = s.snapshot("r2", ["b", "c"])
        self.assertEqual(code, ACCEPTED)
        self.assertEqual(body["status"], "revoking")
        code, body = s.snapshot("r3", ["c", "d"])
        self.assertEqual(code, ACCEPTED)
        self.assertEqual(body["status"], "queued")

        # 第一轮的部分确认不得提前推广第三轮
        code, body = s.confirm("c1", "a", ["0", "2", "4"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "partially_released")
        self.assertNotIn("next_handover", body)
        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r2")
        self.assertIsNotNone(view.get("queued_snapshot"))

        # 最后一次确认：完成 r2 并在同一事务形成 r3 的下一轮交接
        code, body = s.confirm("c2", "b", ["1", "3", "5"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "completed")
        self.assertEqual(body["epoch"], 2)
        nxt = body["next_handover"]
        self.assertEqual(nxt["request_id"], "r3")
        self.assertEqual(nxt["status"], "revoking")
        self.assertEqual(nxt["epoch"], 2)

        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["id"], 2)
        self.assertEqual(view["request_id"], "r3")
        self.assertNotIn("queued_snapshot", view)
        # r3: [c,d] -> 偶号 c、奇号 d；偶号当前在 b 手中，奇号已在 c 手中，
        # 六个分区都仍需当时持有者释放。
        self.assertEqual(
            {r["part"]: (r["owner"], r["target"]) for r in view["revocations"]},
            {
                "0": ("b", "c"), "1": ("c", "d"),
                "2": ("b", "c"), "3": ("c", "d"),
                "4": ("b", "c"), "5": ("c", "d"),
            },
        )
        # 第三轮目标在持有者确认前不可见
        self.assertEqual(s.assignments_view(member="d")["assignments"], {})
        # 已离场的 a 的确认属于越权（当时持有者是 b/c），不能推进新一轮
        code, body = s.confirm("x-old", "a", ["0"])
        self.assertEqual(code, FORBIDDEN)
        self.assertEqual(s.assignments_view()["epoch"], 2)

        # 完成第三轮：b 释放偶号给 c，c 释放奇号给 d
        code, body = s.confirm("c3", "b", ["0", "2", "4"])
        self.assertEqual(code, OK)
        ownership_during = {
            p: v["owner"] for p, v in s.assignments_view()["assignments"].items()
        }
        # 中间读模型：已释放与未释放并存，但单一归属
        self.assertEqual(
            ownership_during,
            {"0": "c", "1": "c", "2": "c", "3": "c", "4": "c", "5": "c"},
        )
        self.assertEqual(body["status"], "partially_released")
        self.assertEqual(body["epoch"], 3)
        code, body = s.confirm("c4", "c", ["1", "3", "5"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "completed")
        self.assertEqual(body["epoch"], 4)
        self.assertNotIn("next_handover", body)
        view = s.assignments_view()
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "c", "1": "d", "2": "c", "3": "d", "4": "c", "5": "d"},
        )
        self.assertFalse(s.handover_view()["active"])
        self._assert_unique_ownership(view)

    def test_chained_handover_converges_identically_after_restart(self) -> None:
        """重启后排队/交接状态不丢失，最终结论保持一致。"""
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.snapshot("r3", ["c", "d"])  # queued
        s.confirm("c1", "a", ["0", "2", "4"])
        # 重启 1：第二轮仍部分进行，排队快照仍在且未被提前推广
        s.close()
        s = self.store()
        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r2")
        self.assertEqual(view["queued_snapshot"]["request_id"], "r3")

        s.confirm("c2", "b", ["1", "3", "5"])
        # 重启 2：第三轮交接已随最后一次确认持久化
        s.close()
        s = self.store()
        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r3")
        self.assertNotIn("queued_snapshot", view)
        self.assertEqual(s.assignments_view(member="d")["assignments"], {})

        s.confirm("c3", "b", ["0", "2", "4"])
        s.confirm("c4", "c", ["1", "3", "5"])
        # 重启 3：最终分配保持
        s.close()
        s = self.store()
        self.assertFalse(s.handover_view()["active"])
        view = s.assignments_view()
        self.assertEqual(view["epoch"], 4)
        self.assertEqual(
            {p: v["owner"] for p, v in view["assignments"].items()},
            {"0": "c", "1": "d", "2": "c", "3": "d", "4": "c", "5": "d"},
        )
        self._assert_unique_ownership(view)

    def test_restart_recovers_queued_snapshot_if_promotion_was_missed(self) -> None:
        """旧版本/异常遗留：无活动交接却留着排队快照时，重启即补推广。"""
        import sqlite3

        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.snapshot("r3", ["c", "d"])  # queued
        s.confirm("c1", "a", ["0", "2", "4"])
        s.close()
        # 手工构造“交接已完成但排队未推广”的遗留状态
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE handover_state SET status='completed' WHERE id=1")
            db.execute(
                "UPDATE assignments SET owner='c', epoch=2"
                " WHERE part IN ('1','3','5')"
            )
            db.execute("DELETE FROM revocations")
            db.execute("UPDATE config SET value='2' WHERE key='epoch'")
            db.commit()
        s = self.store()  # 重启时恢复
        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r3")
        self.assertNotIn("queued_snapshot", view)
        # 基于已持久化归属形成：0/2/4 在 b 手中需让给 c，1/3/5 已在 c 手中需让给 d
        self.assertEqual(
            {r["part"]: (r["owner"], r["target"]) for r in view["revocations"]},
            {
                "0": ("b", "c"), "1": ("c", "d"),
                "2": ("b", "c"), "3": ("c", "d"),
                "4": ("b", "c"), "5": ("c", "d"),
            },
        )
        self._assert_unique_ownership(s.assignments_view())

    def test_queued_snapshot_that_already_matches_promotes_to_stable(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        # r3 与 r2 成员集合相同：r2 完成后归属即满足，推广结果必须是 stable
        self.assertEqual(s.snapshot("r3", ["c", "b"])[0], ACCEPTED)
        code, body = s.confirm("c1", "a", ["0", "2", "4"])
        self.assertNotIn("next_handover", body)
        code, body = s.confirm("c2", "b", ["1", "3", "5"])
        self.assertEqual(code, OK)
        self.assertEqual(body["next_handover"]["status"], "stable")
        self.assertFalse(s.handover_view()["active"])
        # 排队请求重放：返回首次（stable）结果
        code, body = s.snapshot("r3", ["b", "c"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "stable")
        # 重启后仍可重放
        s.close()
        s = self.store()
        code, body = s.snapshot("r3", ["b", "c"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "stable")

    def test_converging_snapshot_after_noop_is_stable(self) -> None:
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.confirm("c1", "a", ["0", "2", "4"])
        s.confirm("c2", "b", ["1", "3", "5"])
        code, body = s.snapshot("r5", ["c", "b"])  # 排序后与 ["b","c"] 同
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "stable")

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

    def test_concurrent_snapshots_open_one_handover_and_queue_one(self) -> None:
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
        by_status: dict[str, int] = {}
        for code, label in results:
            by_status[(str(code), label)] = by_status.get((str(code), label), 0) + 1
        # 恰好一个开启撤销交接，恰好一个受理排队，其余明确冲突
        self.assertEqual(by_status[("202", "revoking")], 1, results)
        self.assertEqual(by_status[("202", "queued")], 1, results)
        self.assertEqual(by_status[("409", "deferred_snapshot_pending")], 13, results)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM revocations").fetchone()[0], 6
            )
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM deferred_snapshot").fetchone()[0], 1
            )
        self._assert_unique_ownership(s.assignments_view())

    def test_concurrent_confirms_promote_queued_snapshot_once(self) -> None:
        """两个持有者并发完成最后确认时，排队快照只形成一次下一轮交接。"""
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.snapshot("r3", ["c", "d"])  # queued
        errors: list[BaseException] = []

        def fire(req_id: str, member: str, parts: list[str]) -> None:
            try:
                code, _ = s.confirm(req_id, member, parts)
                self.assertEqual(code, OK)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=fire, args=("c1", "a", ["0", "2", "4"]))
        t2 = threading.Thread(target=fire, args=("c2", "b", ["1", "3", "5"]))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(errors, [])
        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r3")
        self.assertEqual(len(view["revocations"]), 6)
        self._assert_unique_ownership(s.assignments_view())

    def test_cross_connection_completion_is_observed_and_queued_preserved(self) -> None:
        """独立连接完成前一轮；排队槽在对端完成后仍等待并可继续确认。"""
        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.snapshot("r3", ["c", "d"])
        s.close()
        s1 = self.store()
        s2 = self.store()
        # s1 完成 a 的部分；s2 完成 b 的最后部分 -> 推广 r3
        self.assertEqual(s1.confirm("c1", "a", ["0", "2", "4"])[0], OK)
        code, body = s2.confirm("c2", "b", ["1", "3", "5"])
        self.assertEqual(code, OK)
        self.assertEqual(body["next_handover"]["request_id"], "r3")
        # 两个连接都看到第三轮交接
        self.assertEqual(
            s1.handover_view()["request_id"], "r3"
        )
        self.assertEqual(
            s2.handover_view()["request_id"], "r3"
        )
        self._assert_unique_ownership(s1.assignments_view())
        s1.close()
        s2.close()

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

    def test_crash_on_final_confirm_with_queued_snapshot_rolls_back_together(self) -> None:
        """末次确认（含排队推广）在提交前崩溃：整体回滚，已受理快照不丢失。"""
        import subprocess
        import sys

        s = self.store()
        s.snapshot("r1", ["a", "b"])
        s.snapshot("r2", ["b", "c"])
        s.snapshot("r3", ["c", "d"])  # queued
        s.confirm("c1", "a", ["0", "2", "4"])
        s.close()

        script = (
            "import os, sys; sys.path.insert(0, %r);"
            " from app.store import Store;"
            " s = Store(%r, partition_count=6);"
            " s.confirm('c2', 'b', ['1','3','5'], _crash_hook=lambda: os._exit(42))"
            % (os.path.dirname(os.path.dirname(os.path.abspath(__file__))), self.path)
        )
        proc = subprocess.run(
            [sys.executable, "-c", script], capture_output=True
        )
        self.assertEqual(proc.returncode, 42, proc.stderr.decode())

        # 重启：第二轮仍未完成、排队快照仍在、没有第三轮的半成品
        s = self.store()
        view = s.handover_view()
        self.assertTrue(view["active"])
        self.assertEqual(view["request_id"], "r2")
        self.assertEqual(view["queued_snapshot"]["request_id"], "r3")
        self.assertEqual(
            {p: v["owner"] for p, v in s.assignments_view()["assignments"].items()},
            {"0": "b", "1": "b", "2": "b", "3": "b", "4": "b", "5": "b"},
        )
        # 重传末次确认：正常完成并在同一事务推广 r3
        code, body = s.confirm("c2", "b", ["1", "3", "5"])
        self.assertEqual(code, OK)
        self.assertEqual(body["status"], "completed")
        self.assertEqual(body["remaining"], [])
        self.assertEqual(body["next_handover"]["request_id"], "r3")
        view = s.handover_view()
        self.assertEqual(view["request_id"], "r3")
        self.assertNotIn("queued_snapshot", view)
        self._assert_unique_ownership(s.assignments_view())

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
