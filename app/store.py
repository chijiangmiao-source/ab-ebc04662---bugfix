"""持久化与交接协议核心。

不变量
------
* 任一代次内，一个分区至多归属一个接收实例。
* 撤销中的分区仍记在旧实例名下（读模型看到的是旧完整分配），
  直到旧实例确认；确认时“删除旧所有权 / 转授新实例 / 推进可交接集合 /
  公布新代次”发生在同一个持久化事务中，因此外部观察只可能是：
  旧完整分配，或与已确认释放一致的中间分配。
* 所有写接口以稳定请求标识幂等：相同请求重放首次结果；
  相同请求标识携带不同快照明确冲突。
* 交接进行中受理的后续完整成员快照进入唯一的排队槽（queued）；
  当且仅当前一轮最后一次确认在同一事务内完成发布时，排队快照基于
  已持久化的当前归属立即形成下一轮一致交接（或在已满足时直接 stable），
  排队槽随推广一并清空，崩溃恢复也不会丢失已受理快照。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any

# 业务结果码（与 HTTP 状态对齐）
OK = 200
ACCEPTED = 202
BAD_REQUEST = 400
FORBIDDEN = 403
CONFLICT = 409
SERVICE_UNAVAILABLE = 503


class StoreError(Exception):
    """调用方可直接展示的协议错误。"""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class Store:
    """单连接、进程内加锁的持久化存储。

    数据库层使用 ``BEGIN IMMEDIATE`` 串行化写入，因此即使被多进程/多连接
    并发访问（测试与恢复场景），协议状态仍然一致。
    """

    def __init__(self, path: str, partition_count: int = 256) -> None:
        if partition_count <= 0:
            raise ValueError("partition_count must be positive")
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path,
            isolation_level=None,  # 显式管理事务
            check_same_thread=False,
            timeout=30.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._ensure_schema(partition_count)

    # ------------------------------------------------------------------ schema

    def _ensure_schema(self, partition_count: int) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- 分区当前所有权；owner 为空表示尚未分配。
            -- epoch 为该所有权被（最近一次）公布时的代次。
            CREATE TABLE IF NOT EXISTS assignments (
                part  TEXT PRIMARY KEY,
                owner TEXT,
                epoch INTEGER NOT NULL
            );

            -- 撤销中的分区：旧实例仍持有（assignments.owner 不动），
            -- 目标实例只有在旧实例确认后的同一事务里才能取得所有权。
            CREATE TABLE IF NOT EXISTS revocations (
                part       TEXT PRIMARY KEY REFERENCES assignments(part),
                owner      TEXT NOT NULL,
                target     TEXT NOT NULL,
                epoch      INTEGER NOT NULL,
                req_id     TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            -- 交接状态：每轮一行（id 为递交代次序号），至多一个 pending；
            -- completed 行保留以审计“已持久化目标”，并在重启后继续串联排队快照。
            CREATE TABLE IF NOT EXISTS handover_state (
                id            INTEGER PRIMARY KEY CHECK (id >= 1),
                req_id        TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                target_json   TEXT NOT NULL,
                status        TEXT NOT NULL CHECK (status IN ('pending', 'completed')),
                result_json   TEXT,
                created_at    TEXT NOT NULL,
                completed_at  TEXT
            );

            CREATE TABLE IF NOT EXISTS deferred_snapshot (
                id                INTEGER PRIMARY KEY CHECK (id = 1),
                request_id        TEXT NOT NULL,
                snapshot_json     TEXT NOT NULL,
                after_handover_id TEXT NOT NULL,
                created_at        TEXT NOT NULL
            );

            -- 稳定请求标识 -> 首次结果，用于重传重放与冲突检测。
            CREATE TABLE IF NOT EXISTS requests (
                req_id        TEXT PRIMARY KEY,
                kind          TEXT NOT NULL CHECK (kind IN ('snapshot', 'confirm')),
                snapshot_json TEXT,
                code          INTEGER NOT NULL,
                response_json TEXT NOT NULL,
                created_at    TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_revocations_owner ON revocations(owner);
            CREATE INDEX IF NOT EXISTS idx_assignments_owner ON assignments(owner);
            -- 任意时刻至多一个进行中（pending）交接
            CREATE UNIQUE INDEX IF NOT EXISTS idx_handover_single_active
                ON handover_state(1) WHERE status = 'pending';
            """
        )
        self._migrate_schema()
        row = self._conn.execute(
            "SELECT value FROM config WHERE key = 'epoch'"
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO config(key, value) VALUES ('epoch', '0')"
            )
        row = self._conn.execute(
            "SELECT value FROM config WHERE key = 'partition_count'"
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO config(key, value) VALUES ('partition_count', ?)",
                (str(partition_count),),
            )
        # 崩溃/旧版本遗留恢复：活动交接不存在却仍留着已受理排队快照时，
        # 立即基于持久化归属补做推广，保证已受理快照不丢失、不重复。
        self._recover_queued_snapshot()

    def _migrate_schema(self) -> None:
        conn = self._conn
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table'"
            " AND name = 'handover_state'"
        ).fetchone()
        normalized = "".join((row[0] or "").split()).lower() if row is not None else ""
        if "check(id=1)" in normalized:
            # 旧版 handover_state 限定 id 恒为 1；重建为每轮一行并保留原行。
            conn.executescript(
                """
                ALTER TABLE handover_state RENAME TO handover_state_legacy;
                CREATE TABLE handover_state (
                    id            INTEGER PRIMARY KEY,
                    req_id        TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    target_json   TEXT NOT NULL,
                    status        TEXT NOT NULL CHECK (status IN ('pending', 'completed')),
                    result_json   TEXT,
                    created_at    TEXT NOT NULL,
                    completed_at  TEXT
                );
                INSERT INTO handover_state(id, req_id, snapshot_json, target_json,
                    status, result_json, created_at, completed_at)
                SELECT id, req_id, snapshot_json, target_json, status, result_json,
                    created_at, completed_at FROM handover_state_legacy;
                DROP TABLE handover_state_legacy;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_handover_single_active
                    ON handover_state(1) WHERE status = 'pending';
                """
            )

    def _recover_queued_snapshot(self) -> None:
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            active = self._active_handover(conn)
            queued = conn.execute(
                "SELECT * FROM deferred_snapshot WHERE id = 1"
            ).fetchone()
            if active is None and queued is not None:
                self._promote_queued(
                    conn,
                    queued["request_id"],
                    json.loads(queued["snapshot_json"]),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ helpers

    def _epoch(self, conn: sqlite3.Connection) -> int:
        return int(
            conn.execute("SELECT value FROM config WHERE key = 'epoch'").fetchone()[0]
        )

    def _set_epoch(self, conn: sqlite3.Connection, epoch: int) -> None:
        conn.execute(
            "UPDATE config SET value = ? WHERE key = 'epoch'", (str(epoch),)
        )

    def _partition_count(self, conn: sqlite3.Connection) -> int:
        return int(
            conn.execute(
                "SELECT value FROM config WHERE key = 'partition_count'"
            ).fetchone()[0]
        )

    @staticmethod
    def _validate_request_id(value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise StoreError("request_id 必须是非空字符串")
        return value.strip()

    @staticmethod
    def _validate_members(value: Any) -> list[str]:
        if not isinstance(value, list) or not value:
            raise StoreError("members 必须是非空数组")
        members: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise StoreError("成员标识必须是非空字符串")
            member = item.strip()
            if member in seen:
                raise StoreError(f"成员快照包含重复成员: {member}")
            seen.add(member)
            members.append(member)
        return members

    @staticmethod
    def _validate_parts(value: Any) -> list[str]:
        if not isinstance(value, list) or not value:
            raise StoreError("parts 必须是非空数组")
        parts: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise StoreError("分区号必须是非空字符串")
            part = item.strip()
            if part in seen:
                raise StoreError(f"确认列表包含重复分区: {part}")
            seen.add(part)
            parts.append(part)
        return parts

    @staticmethod
    def _replay(row: sqlite3.Row) -> tuple[int, dict[str, Any]]:
        return int(row["code"]), json.loads(row["response_json"])

    def _record(
        self,
        conn: sqlite3.Connection,
        req_id: str,
        kind: str,
        snapshot: list[str] | None,
        code: int,
        response: dict[str, Any],
    ) -> None:
        conn.execute(
            "INSERT INTO requests(req_id, kind, snapshot_json, code, response_json,"
            " created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                req_id,
                kind,
                _canonical_json(snapshot) if snapshot is not None else None,
                code,
                _canonical_json(response),
                _now(),
            ),
        )

    def _active_handover(
        self, conn: sqlite3.Connection
    ) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM handover_state WHERE status = 'pending'"
            " ORDER BY id DESC LIMIT 1"
        ).fetchone()

    def _latest_handover(
        self, conn: sqlite3.Connection
    ) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM handover_state ORDER BY id DESC LIMIT 1"
        ).fetchone()

    @staticmethod
    def _next_handover_id(conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT MAX(id) FROM handover_state").fetchone()
        return (row[0] or 0) + 1

    def _plan_targets(
        self,
        conn: sqlite3.Connection,
        ordered: list[str],
    ) -> tuple[dict[str, str], list[str], list[dict[str, str]]]:
        """基于已持久化归属，计算成员快照对应的稳定目标与本轮差异。

        返回 ``(target, grants, revokes)``：
        ``grants`` 为当前无主、可直接授予的分区；
        ``revokes`` 为仍需当时持有者释放的分区 ``{part, owner, target}``。
        """
        count = self._partition_count(conn)
        current_rows = conn.execute(
            "SELECT part, owner FROM assignments"
        ).fetchall()
        current = {r["part"]: r["owner"] for r in current_rows}
        parts = (
            [str(i) for i in range(count)]
            if not current
            else list(current)
        )

        def choose(part: str) -> str:
            # 成员标识 + 分区号确定的稳定目标
            return ordered[int(part) % len(ordered)]

        target = {part: choose(part) for part in sorted(parts, key=_part_key)}
        grants: list[str] = []
        revokes: list[dict[str, str]] = []
        for part, new_owner in target.items():
            old_owner = current.get(part)
            if old_owner is None:
                grants.append(part)
            elif old_owner != new_owner:
                # 离开成员的分区与仍在成员间迁移的分区，都只能由持有者释放
                revokes.append(
                    {"part": part, "owner": old_owner, "target": new_owner}
                )
        return target, grants, revokes

    def _promote_queued(
        self,
        conn: sqlite3.Connection,
        req_id: str,
        ordered: list[str],
    ) -> dict[str, Any]:
        """在前一轮完成发布的同一事务内推广已受理的排队快照。

        必须在活动交接已不存在（最后一轮撤销全部释放）时调用；
        基于已持久化的当前归属继续形成下一轮：无差异则直接 stable，
        否则插入新的 pending 交接。返回推广结果（用于响应体的 next_handover）。
        """
        snapshot_key = _canonical_json(ordered)
        target, grants, revokes = self._plan_targets(conn, ordered)
        epoch = self._epoch(conn)

        for part in grants:
            # 无主分区可直接授予；epoch 保持当前公布代次，不借交接推进
            conn.execute(
                "INSERT INTO assignments(part, owner, epoch)"
                " VALUES (?, ?, ?) ON CONFLICT(part) DO"
                " UPDATE SET owner = excluded.owner, epoch = excluded.epoch",
                (part, target[part], epoch),
            )

        promoted: dict[str, Any]
        if not revokes:
            result = {
                "status": "stable",
                "epoch": epoch,
                "request_id": req_id,
                "assigned": sorted(target, key=_part_key),
            }
            self._record(conn, req_id, "snapshot", ordered, OK, result)
            promoted = {"request_id": req_id, "status": "stable", "epoch": epoch}
        else:
            revoke_target = {item["part"]: item["target"] for item in revokes}
            handover_id = self._next_handover_id(conn)
            conn.execute(
                "INSERT INTO handover_state(id, req_id, snapshot_json,"
                " target_json, status, created_at) VALUES (?, ?, ?, ?,"
                " 'pending', ?)",
                (handover_id, req_id, snapshot_key,
                 _canonical_json(revoke_target), _now()),
            )
            for item in revokes:
                conn.execute(
                    "INSERT INTO revocations(part, owner, target, epoch,"
                    " req_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        item["part"],
                        item["owner"],
                        item["target"],
                        epoch,
                        req_id,
                        _now(),
                    ),
                )
            result = {
                "status": "revoking",
                "epoch": epoch,
                "request_id": req_id,
                "assigned_now": sorted(grants, key=_part_key),
                "revocations": sorted(
                    revokes, key=lambda x: _part_key(x["part"])
                ),
                "promoted_from_queue": True,
            }
            self._record(conn, req_id, "snapshot", ordered, ACCEPTED, result)
            promoted = {
                "request_id": req_id,
                "status": "revoking",
                "epoch": epoch,
                "revocations": result["revocations"],
            }

        # 排队槽随推广一并清空；已受理结果已写入 requests，重放不会丢失
        conn.execute("DELETE FROM deferred_snapshot WHERE id = 1")
        return promoted

    def _revocations_by_owner(
        self, conn: sqlite3.Connection
    ) -> dict[str, list[sqlite3.Row]]:
        grouped: dict[str, list[sqlite3.Row]] = {}
        for row in conn.execute("SELECT * FROM revocations ORDER BY part"):
            grouped.setdefault(row["owner"], []).append(row)
        return grouped

    # ------------------------------------------------------------------- reads

    def health(self) -> bool:
        with self._lock:
            try:
                self._conn.execute("SELECT 1").fetchone()
                return True
            except sqlite3.DatabaseError:
                return False

    def assignments_view(self, member: str | None = None) -> dict[str, Any]:
        """对外读模型：旧完整分配，或与已确认释放一致的中间分配。"""
        with self._lock:
            conn = self._conn
            epoch = self._epoch(conn)
            sql = (
                "SELECT a.part, a.owner, a.epoch, r.target AS revoking_target"
                " FROM assignments a LEFT JOIN revocations r ON r.part = a.part"
            )
            params: tuple[Any, ...] = ()
            if member is not None:
                sql += " WHERE a.owner = ?"
                params = (member,)
            sql += " ORDER BY CAST(a.part AS INTEGER), a.part"
            assignments: dict[str, dict[str, Any]] = {}
            for row in conn.execute(sql, params):
                if row["owner"] is None:
                    continue
                assignments[row["part"]] = {
                    "owner": row["owner"],
                    "epoch": row["epoch"],
                    "revoking": row["revoking_target"],
                }
            return {"epoch": epoch, "assignments": assignments}

    def handover_view(self) -> dict[str, Any]:
        with self._lock:
            conn = self._conn
            row = self._latest_handover(conn)
            deferred = conn.execute(
                "SELECT * FROM deferred_snapshot WHERE id = 1"
            ).fetchone()
            epoch = self._epoch(conn)
            if row is None or row["status"] != "pending":
                view: dict[str, Any] = {"active": False, "epoch": epoch}
                if row is not None:
                    view["last_handover"] = {
                        "id": row["id"],
                        "request_id": row["req_id"],
                        "status": row["status"],
                    }
                if deferred is not None:
                    view["queued_snapshot"] = {
                        "request_id": deferred["request_id"],
                        "members": json.loads(deferred["snapshot_json"]),
                        "after_handover": deferred["after_handover_id"],
                    }
                return view
            target: dict[str, str] = json.loads(row["target_json"])
            remaining = {
                r["part"]: {"owner": r["owner"], "target": r["target"]}
                for r in conn.execute("SELECT * FROM revocations ORDER BY part")
            }
            view = {
                "active": True,
                "epoch": epoch,
                "id": row["id"],
                "request_id": row["req_id"],
                "snapshot": json.loads(row["snapshot_json"]),
                "target": target,
                "revocations": [
                    {"part": p, **remaining[p]} for p in sorted(remaining)
                ],
                "released": [p for p in sorted(target) if p not in remaining],
            }
            if deferred is not None:
                view["queued_snapshot"] = {
                    "request_id": deferred["request_id"],
                    "members": json.loads(deferred["snapshot_json"]),
                    "after_handover": deferred["after_handover_id"],
                }
            return view

    # ------------------------------------------------------------------ writes

    def snapshot(
        self, request_id: str, raw_members: Any
    ) -> tuple[int, dict[str, Any]]:
        """提交完整成员快照，返回 (状态码, 响应体)。"""
        with self._lock:
            req_id = self._validate_request_id(request_id)
            members = self._validate_members(raw_members)
            ordered = sorted(members)
            snapshot_key = _canonical_json(ordered)
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                prior = conn.execute(
                    "SELECT * FROM requests WHERE req_id = ?", (req_id,)
                ).fetchone()
                if prior is not None:
                    if (
                        prior["kind"] != "snapshot"
                        or prior["snapshot_json"] != snapshot_key
                    ):
                        body = {
                            "error": "idempotency_conflict",
                            "message": "请求标识已被不同的请求体使用",
                            "request_id": req_id,
                        }
                        conn.execute("ROLLBACK")
                        return CONFLICT, body
                    result = self._replay(prior)
                    conn.execute("COMMIT")
                    return result

                active = self._active_handover(conn)
                deferred = conn.execute(
                    "SELECT * FROM deferred_snapshot WHERE id = 1"
                ).fetchone()

                if active is None and deferred is not None:
                    # 另一进程已完成前一轮但未及推广（或旧版本遗留）：
                    # 先补做推广，再按推广后的状态受理本次请求。
                    queued_req = deferred["request_id"]
                    queued_snap = deferred["snapshot_json"]
                    self._promote_queued(
                        conn, queued_req, json.loads(queued_snap)
                    )
                    if queued_req == req_id:
                        # 本次请求就是被推广的排队快照：按幂等重放处理
                        if queued_snap != snapshot_key:
                            body = {
                                "error": "idempotency_conflict",
                                "message": "请求标识已被不同的成员快照使用",
                                "request_id": req_id,
                            }
                            conn.execute("ROLLBACK")
                            return CONFLICT, body
                        row = conn.execute(
                            "SELECT code, response_json FROM requests"
                            " WHERE req_id = ?",
                            (req_id,),
                        ).fetchone()
                        result = self._replay(row)
                        conn.execute("COMMIT")
                        return result
                    active = self._active_handover(conn)
                    deferred = None

                if active is not None:
                    # 交接进行中：受理至多一份后续完整快照进入排队槽，
                    # 它将在本轮最后一次确认的同一事务里形成下一轮交接。
                    if deferred is not None:
                        if deferred["request_id"] == req_id:
                            if deferred["snapshot_json"] == snapshot_key:
                                # 排队受理结果的稳定重传
                                body = {
                                    "status": "queued",
                                    "request_id": req_id,
                                    "after_handover": active["req_id"],
                                    "target": json.loads(active["target_json"]),
                                }
                                conn.execute("COMMIT")
                                return ACCEPTED, body
                            body = {
                                "error": "idempotency_conflict",
                                "message": "请求标识已被不同的成员快照使用",
                                "request_id": req_id,
                            }
                            conn.execute("ROLLBACK")
                            return CONFLICT, body
                        body = {
                            "error": "deferred_snapshot_pending",
                            "message": "已有成员快照等待当前交接完成",
                            "request_id": req_id,
                            "after_handover": active["req_id"],
                            "queued_request_id": deferred["request_id"],
                        }
                        conn.execute("ROLLBACK")
                        return CONFLICT, body

                    conn.execute(
                        "INSERT INTO deferred_snapshot(id, request_id, snapshot_json,"
                        " after_handover_id, created_at) VALUES (1, ?, ?, ?, ?)",
                        (req_id, snapshot_key, active["req_id"], _now()),
                    )
                    body = {
                        "status": "queued",
                        "request_id": req_id,
                        "after_handover": active["req_id"],
                        "target": json.loads(active["target_json"]),
                    }
                    conn.execute("COMMIT")
                    return ACCEPTED, body

                # 无活动交接：基于已持久化的当前归属计算本轮稳定目标
                epoch = self._epoch(conn)
                target, grants, revokes = self._plan_targets(conn, ordered)

                for part in grants:
                    conn.execute(
                        "INSERT INTO assignments(part, owner, epoch)"
                        " VALUES (?, ?, ?) ON CONFLICT(part) DO"
                        " UPDATE SET owner = excluded.owner, epoch = excluded.epoch",
                        (part, target[part], epoch),
                    )

                if not revokes:
                    # 没有需要旧实例释放的分区：新视图立即生效，不产生代次推进。
                    body = {
                        "status": "stable",
                        "epoch": epoch,
                        "request_id": req_id,
                        "assigned": sorted(target, key=_part_key),
                    }
                    self._record(conn, req_id, "snapshot", ordered, OK, body)
                    conn.execute("COMMIT")
                    return OK, body

                # 交接前沿：仅仍需旧实例释放的分区 -> 新目标
                revoke_target = {item["part"]: item["target"] for item in revokes}
                handover_id = self._next_handover_id(conn)
                conn.execute(
                    "INSERT INTO handover_state(id, req_id, snapshot_json,"
                    " target_json, status, created_at) VALUES (?, ?, ?, ?,"
                    " 'pending', ?)",
                    (handover_id, req_id, snapshot_key,
                     _canonical_json(revoke_target), _now()),
                )
                for item in revokes:
                    conn.execute(
                        "INSERT INTO revocations(part, owner, target, epoch,"
                        " req_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            item["part"],
                            item["owner"],
                            item["target"],
                            epoch,
                            req_id,
                            _now(),
                        ),
                    )
                body = {
                    "status": "revoking",
                    "epoch": epoch,
                    "request_id": req_id,
                    "assigned_now": sorted(grants, key=_part_key),
                    "revocations": sorted(revokes, key=lambda x: _part_key(x["part"])),
                }
                self._record(conn, req_id, "snapshot", ordered, ACCEPTED, body)
                conn.execute("COMMIT")
                return ACCEPTED, body
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def confirm(
        self,
        request_id: str,
        raw_member: Any,
        raw_parts: Any,
        _crash_hook: "callable | None" = None,
    ) -> tuple[int, dict[str, Any]]:
        """旧实例确认撤销分区；所有效果单事务提交。

        ``_crash_hook`` 仅供崩溃恢复测试使用：在释放语句已执行、
        而提交尚未发生时调用（钩子内可直接 ``os._exit`` 杀死进程）。
        """
        with self._lock:
            req_id = self._validate_request_id(request_id)
            if not isinstance(raw_member, str) or not raw_member.strip():
                raise StoreError("member 必须是非空字符串")
            member = raw_member.strip()
            parts = self._validate_parts(raw_parts)
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                prior = conn.execute(
                    "SELECT * FROM requests WHERE req_id = ?", (req_id,)
                ).fetchone()
                if prior is not None:
                    if prior["kind"] != "confirm":
                        body = {
                            "error": "idempotency_conflict",
                            "message": "请求标识已被不同的请求体使用",
                            "request_id": req_id,
                        }
                        conn.execute("ROLLBACK")
                        return CONFLICT, body
                    result = self._replay(prior)
                    conn.execute("COMMIT")
                    return result

                def finish(code: int, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
                    self._record(conn, req_id, "confirm", None, code, body)
                    if _crash_hook is not None and code == OK:
                        _crash_hook()
                    conn.execute("COMMIT")
                    return code, body

                # 已受理排队快照占用的请求标识不能再用于确认：
                # 否则推广时会与幂等记录冲突，并让“复用标识”语义含混。
                # 注意：该拒绝不得写入 requests —— 标识仍归排队快照所有，
                # 排队快照的稳定重放与随后的推广都不能被污染。
                queued_id_row = conn.execute(
                    "SELECT request_id FROM deferred_snapshot WHERE id = 1"
                ).fetchone()
                if queued_id_row is not None and queued_id_row["request_id"] == req_id:
                    body = {
                        "error": "idempotency_conflict",
                        "message": "请求标识已被排队等待的成员快照使用",
                        "request_id": req_id,
                    }
                    conn.execute("ROLLBACK")
                    return CONFLICT, body

                active = self._active_handover(conn)
                if active is None:
                    return finish(
                        CONFLICT,
                        {
                            "error": "confirmation_expired",
                            "message": "没有进行中的交接，确认已过期",
                            "request_id": req_id,
                        },
                    )

                rows = conn.execute(
                    "SELECT * FROM revocations WHERE part IN (%s)"
                    % ",".join("?" * len(parts)),
                    parts,
                ).fetchall()
                by_part = {r["part"]: r for r in rows}

                foreign = [
                    p for p in parts if p in by_part and by_part[p]["owner"] != member
                ]
                if foreign:
                    return finish(
                        FORBIDDEN,
                        {
                            "error": "not_owner",
                            "message": "分区并非由该实例持有，禁止越权确认",
                            "request_id": req_id,
                            "partitions": sorted(foreign, key=_part_key),
                        },
                    )
                extra = [p for p in parts if p not in by_part]
                if extra:
                    return finish(
                        BAD_REQUEST,
                        {
                            "error": "unexpected_partitions",
                            "message": "确认包含不属于本次撤销的多余分区",
                            "request_id": req_id,
                            "partitions": sorted(extra, key=_part_key),
                        },
                    )

                # ---- 临界区：释放旧所有权、转授、推进、公布代次，一次提交 ----
                new_epoch = self._epoch(conn) + 1
                for part in parts:
                    rec = by_part[part]
                    conn.execute(
                        "UPDATE assignments SET owner = ?, epoch = ? WHERE part = ?",
                        (rec["target"], new_epoch, part),
                    )
                    conn.execute("DELETE FROM revocations WHERE part = ?", (part,))

                # 已持久化目标保持不变：released 由“目标 - 仍在撤销表”推导，
                # 交接完成后仍可据此审计与重新收敛。
                remaining_parts = [
                    r["part"]
                    for r in conn.execute(
                        "SELECT part FROM revocations ORDER BY part"
                    )
                ]
                self._set_epoch(conn, new_epoch)

                if not remaining_parts:
                    conn.execute(
                        "UPDATE handover_state SET status = 'completed',"
                        " completed_at = ?, result_json = ? WHERE id = ?",
                        (
                            _now(),
                            _canonical_json(
                                {
                                    "status": "completed",
                                    "epoch": new_epoch,
                                    "request_id": active["req_id"],
                                }
                            ),
                            active["id"],
                        ),
                    )
                    status = "completed"

                    # 前一轮最后一次确认：在同一事务内把已受理的排队快照
                    # 基于已持久化的当前归属继续形成下一轮一致交接。
                    # 提交失败则整体回滚：既不丢已受理快照，也不产生半成品。
                    queued = conn.execute(
                        "SELECT * FROM deferred_snapshot WHERE id = 1"
                    ).fetchone()
                    if queued is not None:
                        promoted = self._promote_queued(
                            conn,
                            queued["request_id"],
                            json.loads(queued["snapshot_json"]),
                        )
                    else:
                        promoted = None
                else:
                    status = "partially_released"
                    promoted = None

                body = {
                    "status": status,
                    "epoch": new_epoch,
                    "request_id": req_id,
                    "confirmed": sorted(parts, key=_part_key),
                    # 本轮确认后仍待释放的分区；在推广下一轮之前取定，
                    # completed 时恒为 []，下一轮待撤销集合见 next_handover。
                    "remaining": remaining_parts,
                }
                if promoted is not None:
                    body["next_handover"] = promoted
                return finish(OK, body)
            except Exception:
                conn.execute("ROLLBACK")
                raise


def _part_key(part: str) -> tuple[int, int, str]:
    """分区排序：数字分区号按数值，其余退化为字典序。"""
    try:
        return (0, int(part), part)
    except ValueError:
        return (1, 0, part)
