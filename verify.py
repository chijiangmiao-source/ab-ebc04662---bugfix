"""verify 单次容器入口。

依次执行：
1. 语法/构建自检（编译全部模块）
2. 交接规则测试（成员替换、失效确认、幂等冲突、并发、中断恢复）
3. HTTP API 冒烟（真实拉起服务进程，走健康端点与完整交接流程）

任一环节失败即以非零状态码退出。
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import unittest

ROOT = os.path.dirname(os.path.abspath(__file__))


def step(title: str) -> None:
    print(f"\n=== verify: {title} ===", flush=True)


def compile_sources() -> bool:
    step("镜像构建自检（py_compile）")
    proc = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "app", "verify.py"]
    )
    return proc.returncode == 0


def run_rule_tests() -> bool:
    step("规则测试（成员替换 / 失效确认 / 中断恢复）")
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for module in ("tests.test_protocol", "tests.test_http_api"):
        suite.addTests(loader.loadTestsFromName(module))
    runner = unittest.TextTestRunner(verbosity=1)
    return runner.run(suite).wasSuccessful()


def http_request(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if body is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def run_api_smoke() -> bool:
    step("API 冒烟（真实服务进程：连续三次快照 + 撤销期间受理 + 重启收敛）")
    tmp = tempfile.TemporaryDirectory()
    port = int(os.environ.get("SMOKE_PORT", "18080"))
    db_path = os.path.join(tmp.name, "smoke.db")
    base = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "HOST": "127.0.0.1",
        "PORT": str(port),
        "DB_PATH": db_path,
        "PARTITION_COUNT": "6",
        "QUIET": "1",
    }
    ok = False

    def start_server() -> subprocess.Popen:
        proc = subprocess.Popen(
            [sys.executable, "-m", "app.server"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        for _ in range(50):
            try:
                code, body = http_request("GET", f"{base}/healthz")
                if code == 200 and body.get("status") == "ok":
                    return proc
            except OSError:
                pass
            time.sleep(0.1)
        raise AssertionError("服务健康端点未在超时内就绪")

    def stop_server(proc: subprocess.Popen) -> None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    proc = start_server()
    try:
        # 第一次快照：稳定分配
        code, _ = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r1", "members": ["a", "b"]},
        )
        assert code == 200, code
        # 第二次快照：触发撤销
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r2", "members": ["b", "c"]},
        )
        assert code == 202 and body["status"] == "revoking", body
        assert len(body["revocations"]) == 6, body
        # 第三次快照：撤销期间被受理（queued），携带已持久化目标
        code, queued = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r3", "members": ["c", "d"]},
        )
        assert code == 202 and queued["status"] == "queued", queued
        assert queued["after_handover"] == "smoke-r2", queued

        # 冲突规则：复用标识但成员不同 / 已有另一份等待快照
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r3", "members": ["c", "e"]},
        )
        assert code == 409 and body["error"] == "idempotency_conflict", body
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r4", "members": ["e", "f"]},
        )
        assert code == 409 and body["error"] == "deferred_snapshot_pending", body
        # 排队标识不能用于确认
        code, body = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-r3", "member": "a", "parts": ["0"]},
        )
        assert code == 409 and body["error"] == "idempotency_conflict", body
        # 相同标识 + 相同成员快照稳定重放排队受理
        code, replay = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r3", "members": ["c", "d"]},
        )
        assert code == 202 and replay == queued, (replay, queued)

        # 失效确认：越权与多余分区必须被拒绝且不推进
        code, body = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-x1", "member": "c", "parts": ["0"]},
        )
        assert code == 403 and body["error"] == "not_owner", body
        code, body = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-x2", "member": "a", "parts": ["7"]},
        )
        assert code == 400 and body["error"] == "unexpected_partitions", body
        code, view = http_request("GET", f"{base}/v1/assignments")
        assert view["epoch"] == 0 and view["assignments"]["0"]["owner"] == "a", view
        # 幂等冲突：同 snapshot id 不同快照
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r1", "members": ["x"]},
        )
        assert code == 409, body

        # 第二轮第一次合法确认：部分释放，排队快照不得提前推广
        code, body = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-c1", "member": "a", "parts": ["0", "2", "4"]},
        )
        assert code == 200 and body["status"] == "partially_released", body
        assert "next_handover" not in body, body
        code, replay = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-c1", "member": "a", "parts": ["0", "2", "4"]},
        )
        assert replay == body, "重传必须返回首次结果"

        # 读模型单一归属：已释放分区归 b，未释放仍在 b（撤销目标 c）
        code, view = http_request("GET", f"{base}/v1/assignments")
        owners = [v["owner"] for v in view["assignments"].values()]
        assert len(owners) == len(set(view["assignments"])), view
        assert all(v["owner"] for v in view["assignments"].values()), view

        # 第二轮最后一次确认：完成并在同一事务形成第三轮交接
        code, body = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-c2", "member": "b", "parts": ["1", "3", "5"]},
        )
        assert code == 200 and body["status"] == "completed", body
        nxt = body["next_handover"]
        assert nxt["request_id"] == "smoke-r3" and nxt["status"] == "revoking", body

        code, hv = http_request("GET", f"{base}/v1/handover")
        assert code == 200 and hv["active"] and hv["request_id"] == "smoke-r3", hv
        assert "queued_snapshot" not in hv, hv
        assert {r["part"]: r["target"] for r in hv["revocations"]} == {
            "0": "c", "1": "d", "2": "c", "3": "d", "4": "c", "5": "d"
        }, hv
        code, view = http_request("GET", f"{base}/v1/assignments?member=d")
        assert view["assignments"] == {}, view

        # 重启：第三轮交接与最终结论必须保持
        stop_server(proc)
        proc = start_server()
        code, hv = http_request("GET", f"{base}/v1/handover")
        assert hv["active"] and hv["request_id"] == "smoke-r3", hv
        for req_id, member, parts in (
            ("smoke-c3", "b", ["0", "2", "4"]),
            ("smoke-c4", "c", ["1", "3", "5"]),
        ):
            code, body = http_request(
                "POST", f"{base}/v1/confirms",
                {"request_id": req_id, "member": member, "parts": parts},
            )
            assert code == 200, body
        code, view = http_request("GET", f"{base}/v1/assignments")
        owners = {p: v["owner"] for p, v in view["assignments"].items()}
        assert view["epoch"] == 4, view
        assert owners == {
            "0": "c", "1": "d", "2": "c", "3": "d", "4": "c", "5": "d"
        }, owners

        # 再重启：结论不变，已受理快照不丢失
        stop_server(proc)
        proc = start_server()
        code, hv = http_request("GET", f"{base}/v1/handover")
        assert code == 200 and not hv["active"], hv
        code, view = http_request("GET", f"{base}/v1/assignments")
        owners = {p: v["owner"] for p, v in view["assignments"].items()}
        assert owners == {
            "0": "c", "1": "d", "2": "c", "3": "d", "4": "c", "5": "d"
        }, owners
        print(
            "API 冒烟通过：排队受理、冲突规则、撤销隔离、失效拒绝、幂等重放、"
            "末次确认串联下一轮、重启收敛、单一归属"
        )
        ok = True
    except AssertionError as exc:
        print(f"API 冒烟失败: {exc}", flush=True)
        try:
            print(proc.stdout.read().decode(), flush=True)
        except Exception:
            pass
    finally:
        stop_server(proc)
        tmp.cleanup()
    return ok


def main() -> int:
    results = [
        compile_sources(),
        run_rule_tests(),
        run_api_smoke(),
    ]
    step("总结")
    labels = ("镜像构建自检", "规则测试", "API 冒烟")
    for label, ok in zip(labels, results):
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
