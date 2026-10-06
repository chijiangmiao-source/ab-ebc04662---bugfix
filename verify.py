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
    step("API 冒烟（真实服务进程，含三次连续快照与重启）")
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "smoke.db")
    port = int(os.environ.get("SMOKE_PORT", "18080"))

    def start() -> subprocess.Popen:
        env = {
            **os.environ,
            "HOST": "127.0.0.1",
            "PORT": str(port),
            "DB_PATH": db_path,
            "PARTITION_COUNT": "6",
            "QUIET": "1",
        }
        return subprocess.Popen(
            [sys.executable, "-m", "app.server"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

    def wait_ready(proc: subprocess.Popen) -> None:
        for _ in range(50):
            try:
                code, body = http_request("GET", f"{base}/healthz")
                if code == 200 and body.get("status") == "ok":
                    return
            except OSError:
                pass
            time.sleep(0.1)
        raise AssertionError("服务健康端点未在超时内就绪")

    def stop(proc: subprocess.Popen) -> None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    base = f"http://127.0.0.1:{port}"
    proc = start()
    ok = False
    try:
        wait_ready(proc)

        code, _ = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r1", "members": ["a", "b"]},
        )
        assert code == 200, code
        # 第二次快照触发撤销
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r2", "members": ["b", "c"]},
        )
        assert code == 202 and len(body["revocations"]) == 6, body

        # 第三次快照在撤销期间被受理排队
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r3", "members": ["c", "d"]},
        )
        assert code == 202 and body["status"] == "queued", body
        assert body["after_handover"] == "smoke-r2", body
        # 相同标识+相同成员（乱序）稳定重放 queued
        code, replay = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r3", "members": ["d", "c"]},
        )
        assert code == 202 and replay == body, (code, replay, body)
        # 复用标识但成员不同 -> 冲突
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r3", "members": ["c", "z"]},
        )
        assert code == 409 and body["error"] == "idempotency_conflict", body
        # 已有另一份等待快照 -> 冲突
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r4", "members": ["e", "f"]},
        )
        assert code == 409 and body["error"] == "deferred_snapshot_pending", body

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

        # 幂等：同 id 不同快照冲突
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r1", "members": ["x"]},
        )
        assert code == 409, body

        # 合法确认完成 r2；最后一次确认的同事务内开启 r3 交接
        code, body = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-c1", "member": "a", "parts": ["0", "2", "4"]},
        )
        assert code == 200 and body["status"] == "partially_released", body
        code, body = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-c2", "member": "b", "parts": ["1", "3", "5"]},
        )
        assert code == 200 and body["status"] == "completed", body
        assert body["next_handover"]["request_id"] == "smoke-r3", body

        # r3 成员集合已开始其应有交接
        code, hv = http_request("GET", f"{base}/v1/handover")
        assert code == 200 and hv["active"] and hv["request_id"] == "smoke-r3", hv
        assert "queued_snapshot" not in hv, hv
        assert {r["part"]: (r["owner"], r["target"]) for r in hv["revocations"]} == {
            "0": ("b", "c"), "1": ("c", "d"),
            "2": ("b", "c"), "3": ("c", "d"),
            "4": ("b", "c"), "5": ("c", "d"),
        }, hv

        # ---- 重启：已受理快照与 r3 交接结论保持 ----
        stop(proc)
        proc = start()
        wait_ready(proc)
        code, hv = http_request("GET", f"{base}/v1/handover")
        assert code == 200 and hv["active"] and hv["request_id"] == "smoke-r3", hv
        assert "queued_snapshot" not in hv, hv
        code, view = http_request("GET", f"{base}/v1/assignments")
        assert view["epoch"] == 2, view

        # 旧轮次越权确认不得推进新一轮
        code, body = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-old1", "member": "a", "parts": ["0"]},
        )
        assert code == 403 and body["error"] == "not_owner", body

        # 完成 r3
        for req_id, member, parts in (
            ("smoke-c3", "b", ["0", "2", "4"]),
            ("smoke-c4", "c", ["1", "3", "5"]),
        ):
            code, body = http_request(
                "POST", f"{base}/v1/confirms",
                {"request_id": req_id, "member": member, "parts": parts},
            )
            assert code == 200, body
            replay_code, replay = http_request(
                "POST", f"{base}/v1/confirms",
                {"request_id": req_id, "member": member, "parts": parts},
            )
            assert replay_code == 200 and replay == body, "重传必须返回首次结果"

        code, view = http_request("GET", f"{base}/v1/assignments")
        owners = {p: v["owner"] for p, v in view["assignments"].items()}
        assert owners == {
            "0": "c", "1": "d", "2": "c", "3": "d", "4": "c", "5": "d"
        }, owners

        # ---- 再次重启，最终单一归属稳定不变 ----
        stop(proc)
        proc = start()
        wait_ready(proc)
        code, hv = http_request("GET", f"{base}/v1/handover")
        assert code == 200 and not hv["active"], hv
        code, view = http_request("GET", f"{base}/v1/assignments")
        owners = {p: v["owner"] for p, v in view["assignments"].items()}
        assert owners == {
            "0": "c", "1": "d", "2": "c", "3": "d", "4": "c", "5": "d"
        }, owners
        # 读模型单一归属：分区唯一且 owner 非空
        assert len(view["assignments"]) == 6 and all(
            v["owner"] for v in view["assignments"].values()
        ), view
        print(
            "API 冒烟通过：健康检查、三次连续快照受理排队、撤销隔离、失效拒绝、"
            "幂等重放、连续交接、重启收敛与单一归属"
        )
        ok = True
    except AssertionError as exc:
        print(f"API 冒烟失败: {exc}", flush=True)
        try:
            print(proc.stdout.read().decode(), flush=True)
        except Exception:
            pass
    finally:
        stop(proc)
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
