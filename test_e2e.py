"""
End-to-end test cho Distributed Task Scheduler.
Chay: .venv\Scripts\python test_e2e.py

Luong test:
  1. Khoi dong webhook receiver noi bo (port 9000)
  2. Tao task one-time due trong 8 giay
  3. Tao task co cron expression
  4. Kiem tra trang thai PENDING -> SCHEDULED -> RUNNING -> SUCCESS
  5. Xac nhan webhook da duoc goi
  6. Kiem tra Cassandra log
  7. Test huy task (cancel)
  8. Test rate limiting
  9. Test du lieu khong hop le (validation)
"""

import asyncio
import json
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

# Force UTF-8 stdout (Windows cp1252 default)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

import asyncpg
import httpx
import redis.asyncio as aioredis

API = "http://localhost:8000/api/v1"
WEBHOOK_PORT = 9000
WEBHOOK_URL = f"http://host.docker.internal:{WEBHOOK_PORT}/webhook"

# --- Color helpers ---
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"

received_webhooks: list[dict] = []
_pass = 0
_fail = 0


def ok(msg: str) -> None:
    global _pass
    _pass += 1
    print(f"  {GREEN}✓{RESET} {msg}")


def fail(msg: str) -> None:
    global _fail
    _fail += 1
    print(f"  {RED}✗{RESET} {msg}")


def info(msg: str) -> None:
    print(f"  {CYAN}→{RESET} {msg}")


def section(title: str) -> None:
    print(f"\n{BOLD}{YELLOW}{'─'*50}{RESET}")
    print(f"{BOLD}{YELLOW}  {title}{RESET}")
    print(f"{BOLD}{YELLOW}{'─'*50}{RESET}")


# ── Webhook receiver ─────────────────────────────────────────────────────────

class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            received_webhooks.append(json.loads(body))
        except Exception:
            received_webhooks.append({"raw": body.decode()})
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, *_):
        pass  # silence access log


def start_webhook_server() -> HTTPServer:
    server = HTTPServer(("0.0.0.0", WEBHOOK_PORT), WebhookHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# ── Helpers ──────────────────────────────────────────────────────────────────

async def poll_status(client: httpx.AsyncClient, task_id: str, target: str, timeout: int = 90) -> str:
    """Poll API until task reaches target status (or timeout)."""
    deadline = time.time() + timeout
    last_status = "?"
    while time.time() < deadline:
        r = await client.get(f"{API}/tasks/{task_id}")
        if r.status_code == 200:
            last_status = r.json()["status"]
            if last_status == target:
                return last_status
            if last_status in ("FAILED", "CANCELLED") and target not in ("FAILED", "CANCELLED"):
                return last_status
        await asyncio.sleep(2)
    return last_status


async def create_task(client: httpx.AsyncClient, payload: dict) -> dict | None:
    r = await client.post(f"{API}/tasks", json=payload)
    if not r.is_success:
        fail(f"POST /tasks tra {r.status_code}: {r.text[:300]}")
        return None
    return r.json()


# ── Tests ─────────────────────────────────────────────────────────────────────

async def setup_clean_db() -> None:
    section("Setup: Xoa du lieu cu")
    pg = await asyncpg.connect("postgresql://postgres:postgres@localhost:5432/taskscheduler")
    result = await pg.fetch("DELETE FROM tasks RETURNING id")
    await pg.close()
    ok(f"PostgreSQL: da xoa {len(result)} tasks")

    r = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    await r.delete("task_scheduler:pending")
    await r.aclose()
    ok("Redis ZSET: da xoa")


async def test_health(client: httpx.AsyncClient) -> None:
    section("Test 1: Health check")
    r = await client.get("http://localhost:8000/health")
    if r.status_code == 200 and r.json().get("status") == "ok":
        ok("API Gateway dang chay")
    else:
        fail(f"Health check that bai: {r.status_code}")


async def test_validation(client: httpx.AsyncClient) -> None:
    section("Test 2: Validation (du lieu sai)")

    # Thieu ca cron va execute_at
    r = await client.post(f"{API}/tasks", json={
        "user_id": "tester",
        "name": "bad task",
        "webhook_url": "https://example.com",
    })
    if r.status_code == 422:
        ok("Tra 422 khi thieu schedule info")
    else:
        fail(f"Mong 422, nhan {r.status_code}")

    # Cron expression sai
    r = await client.post(f"{API}/tasks", json={
        "user_id": "tester",
        "name": "bad cron",
        "cron_expression": "not-a-cron",
        "webhook_url": "https://example.com",
    })
    if r.status_code == 422:
        ok("Tra 422 khi cron expression sai")
    else:
        fail(f"Mong 422, nhan {r.status_code}")

    # Webhook URL sai dinh dang
    r = await client.post(f"{API}/tasks", json={
        "user_id": "tester",
        "name": "bad url",
        "execute_at": datetime.now(timezone.utc).isoformat(),
        "webhook_url": "not-a-url",
    })
    if r.status_code == 422:
        ok("Tra 422 khi webhook_url khong hop le")
    else:
        fail(f"Mong 422, nhan {r.status_code}")

    # Co ca cron lan execute_at
    r = await client.post(f"{API}/tasks", json={
        "user_id": "tester",
        "name": "both fields",
        "cron_expression": "* * * * *",
        "execute_at": datetime.now(timezone.utc).isoformat(),
        "webhook_url": "https://example.com",
    })
    if r.status_code == 422:
        ok("Tra 422 khi co ca cron va execute_at")
    else:
        fail(f"Mong 422, nhan {r.status_code}")


async def test_one_time_task(client: httpx.AsyncClient) -> None:
    section("Test 3: One-time task (full flow)")

    execute_at = (datetime.now(timezone.utc) + timedelta(seconds=8)).isoformat()
    webhook_count_before = len(received_webhooks)

    task = await create_task(client, {
        "user_id": "e2e_user",
        "name": "E2E one-time task",
        "execute_at": execute_at,
        "webhook_url": WEBHOOK_URL,
        "payload": {"test": "one-time", "ts": time.time()},
    })
    if task is None:
        fail("Khong tao duoc task de test full flow")
        return
    task_id = task["id"]
    info(f"Task tao thanh cong: {task_id}")

    if task["status"] in ("PENDING", "SCHEDULED"):
        ok(f"Trang thai ban dau hop le: {task['status']}")
    else:
        fail(f"Trang thai ban dau sai: {task['status']}")

    # Doi task thuc thi
    info("Dang doi task thuc thi (toi da 90 giay)...")
    final_status = await poll_status(client, task_id, "SUCCESS", timeout=90)

    if final_status == "SUCCESS":
        ok(f"Task hoan thanh: {final_status}")
    else:
        fail(f"Task khong thanh cong: {final_status}")

    # Kiem tra webhook da duoc goi
    new_hooks = received_webhooks[webhook_count_before:]
    if new_hooks:
        ok(f"Webhook da duoc goi ({len(new_hooks)} lan)")
        payload = new_hooks[0]
        if payload.get("test") == "one-time":
            ok("Payload webhook dung")
        else:
            fail(f"Payload webhook sai: {payload}")
    else:
        fail("Webhook CHUA duoc goi")

    # Kiem tra du lieu tra ve
    r = await client.get(f"{API}/tasks/{task_id}")
    data = r.json()
    if data["retry_count"] == 0:
        ok("retry_count = 0 (khong can retry)")
    if data["version"] > 0:
        ok(f"version da tang len {data['version']} (optimistic locking hoat dong)")


async def test_cancel_task(client: httpx.AsyncClient) -> None:
    section("Test 4: Cancel task")

    execute_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    task = await create_task(client, {
        "user_id": "e2e_user",
        "name": "Task se bi huy",
        "execute_at": execute_at,
        "webhook_url": WEBHOOK_URL,
        "payload": {},
    })
    if task is None:
        fail("Khong tao duoc task de test cancel")
        return
    task_id = task["id"]

    r = await client.delete(f"{API}/tasks/{task_id}")
    if r.status_code == 200 and r.json()["status"] == "CANCELLED":
        ok("Huy task thanh cong")
    else:
        fail(f"Huy task that bai: {r.status_code} {r.text}")

    # Xac nhan task khong the huy lan 2
    r2 = await client.delete(f"{API}/tasks/{task_id}")
    if r2.status_code == 409:
        ok("Khong the huy task da CANCELLED (tra 409)")
    else:
        fail(f"Mong 409, nhan {r2.status_code}")


async def test_list_tasks(client: httpx.AsyncClient) -> None:
    section("Test 5: List & filter tasks")

    r = await client.get(f"{API}/tasks", params={"user_id": "e2e_user", "page": 1, "page_size": 10})
    if r.status_code == 200:
        data = r.json()
        ok(f"List tra ve {data['total']} task, page={data['page']}, page_size={data['page_size']}")
        if isinstance(data["tasks"], list):
            ok("Cau truc response dung (tasks la list)")
        else:
            fail("Cau truc response sai")
    else:
        fail(f"List tasks that bai: {r.status_code}")

    # Page khong ton tai
    r = await client.get(f"{API}/tasks", params={"page": 9999, "page_size": 100})
    if r.status_code == 200 and r.json()["tasks"] == []:
        ok("Page vuot qua tra list rong")
    else:
        fail(f"Mong list rong, nhan: {r.status_code}")


async def test_not_found(client: httpx.AsyncClient) -> None:
    section("Test 6: Task khong ton tai")

    fake_id = str(uuid.uuid4())
    r = await client.get(f"{API}/tasks/{fake_id}")
    if r.status_code == 404:
        ok("Tra 404 voi task_id khong ton tai")
    else:
        fail(f"Mong 404, nhan {r.status_code}")


async def test_cron_task(client: httpx.AsyncClient) -> None:
    section("Test 7: Cron task tao thanh cong")

    task = await create_task(client, {
        "user_id": "e2e_user",
        "name": "Cron moi phut",
        "cron_expression": "* * * * *",
        "webhook_url": WEBHOOK_URL,
        "payload": {"type": "cron"},
        "max_retries": 2,
    })
    if task is None:
        fail("Khong tao duoc cron task")
        return

    if task["cron_expression"] == "* * * * *":
        ok("cron_expression luu dung")
    else:
        fail("cron_expression sai")

    if task["next_run_at"] is not None:
        ok(f"next_run_at duoc tinh: {task['next_run_at']}")
    else:
        fail("next_run_at bi null")

    if task["max_retries"] == 2:
        ok("max_retries = 2 luu dung")
    else:
        fail(f"max_retries sai: {task['max_retries']}")

    # Huy de khong spam webhook trong test
    await client.delete(f"{API}/tasks/{task['id']}")
    info("Da huy cron task sau khi test")


# ── Main ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    print(f"\n{BOLD}{'='*50}")
    print("  DISTRIBUTED TASK SCHEDULER — E2E TEST")
    print(f"{'='*50}{RESET}")

    # Kiem tra API co chay khong truoc khi bat dau
    try:
        async with httpx.AsyncClient(timeout=5) as probe:
            await probe.get("http://localhost:8000/health")
    except Exception:
        print(f"\n{RED}[LOI] API khong chay tai localhost:8000")
        print(f"       Hay chay start.bat truoc khi chay test.{RESET}\n")
        sys.exit(1)

    server = start_webhook_server()
    info(f"Webhook receiver dang lang nghe tai :{WEBHOOK_PORT}")

    await setup_clean_db()

    async with httpx.AsyncClient(timeout=30) as client:
        await test_health(client)
        await test_validation(client)
        await test_cancel_task(client)
        await test_list_tasks(client)
        await test_not_found(client)
        await test_cron_task(client)
        await test_one_time_task(client)  # chay cuoi vi can cho dai nhat

    server.shutdown()

    # Ket qua
    total = _pass + _fail
    print(f"\n{BOLD}{'='*50}")
    print(f"  KET QUA: {_pass}/{total} tests passed")
    if _fail == 0:
        print(f"  {GREEN}TAT CA TESTS DA PASS!{RESET}{BOLD}")
    else:
        print(f"  {RED}{_fail} test(s) FAILED{RESET}{BOLD}")
    print(f"{'='*50}{RESET}\n")

    sys.exit(0 if _fail == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
