# Distributed Task Scheduler

Hệ thống lập lịch tác vụ phân tán theo kiến trúc 9 bước.

## Kiến trúc tổng quan

```
Client → [API Gateway / Task Management]
              │
              ▼
         PostgreSQL (Source of Truth, PENDING)
              │
              ├──► Redis ZSET (nếu due ≤ 15 phút)   ← Bước 4 (nhanh)
              │
         [Relay Service] ─────────────────────────► Redis ZSET   ← Bước 5 (mỗi 5 phút)
                                                        │
                                              [Scheduler, 100ms poll]
                                              Lua atomic pop        ← Bước 6
                                                        │
                                                        ▼
                                                   RabbitMQ         ← Bước 7
                                              (sharding by user_id)
                                                        │
                                                   [Workers]        ← Bước 8
                                              Optimistic Locking
                                                        │
                                          ┌─────────────┴──────────────┐
                                          ▼                            ▼
                                     PostgreSQL                   Cassandra
                                (SUCCESS / FAILED)           (Audit Logs)  ← Bước 9
```

## Yêu cầu

- Python 3.11+
- Docker & Docker Compose

## Cài đặt

```bash
# 1. Khởi động infrastructure
docker-compose up -d

# 2. Cài thư viện Python
pip install -r requirements.txt

# 3. Copy cấu hình
cp .env .env.local   # chỉnh sửa nếu cần
```

## Chạy hệ thống

Mở **4 terminal** khác nhau:

```bash
# Terminal 1 — API Gateway (port 8000)
python main_api.py

# Terminal 2 — Relay Service (PostgreSQL → Redis, mỗi 5 phút)
python main_relay.py

# Terminal 3 — Scheduler (Redis → RabbitMQ, mỗi 100ms)
python main_scheduler.py

# Terminal 4 — Worker (RabbitMQ → webhook → DB + Cassandra)
python main_worker.py
```

## Sử dụng API

### Đăng ký tác vụ một lần

```bash
curl -X POST http://localhost:8000/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user123",
    "name": "Send report",
    "execute_at": "2025-01-01T10:00:00Z",
    "webhook_url": "https://webhook.site/your-id",
    "payload": {"report_type": "daily"}
  }'
```

### Đăng ký tác vụ định kỳ (Cron)

```bash
curl -X POST http://localhost:8000/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user123",
    "name": "Hourly sync",
    "cron_expression": "0 * * * *",
    "webhook_url": "https://webhook.site/your-id",
    "payload": {"action": "sync"}
  }'
```

### Xem danh sách tác vụ

```bash
curl "http://localhost:8000/api/v1/tasks?user_id=user123&page=1&page_size=20"
```

### Xem chi tiết tác vụ

```bash
curl http://localhost:8000/api/v1/tasks/{task_id}
```

### Huỷ tác vụ

```bash
curl -X DELETE http://localhost:8000/api/v1/tasks/{task_id}
```

### Swagger UI

Truy cập http://localhost:8000/docs để xem toàn bộ API.

## Chi tiết từng thành phần

| File | Vai trò | Bước |
|------|---------|------|
| `src/api/app.py` | API Gateway (FastAPI + rate limiting) | 1 |
| `src/api/routes/tasks.py` | Task Management routes | 2 |
| `src/services/task_service.py` | Validate, lưu PostgreSQL, buffer Redis | 3, 4 |
| `src/services/relay_service.py` | Quét PostgreSQL → Redis ZSET | 5 |
| `src/services/scheduler_service.py` | Lua atomic pop → RabbitMQ | 6, 7 |
| `src/services/worker_service.py` | Execute webhook + log Cassandra | 8, 9 |
| `src/scripts/lua/atomic_pop.lua` | Atomic batch pop tránh trùng lặp | 6 |

## Các tính năng kỹ thuật

- **Rate Limiting**: `slowapi` giới hạn 60 req/phút/IP
- **Optimistic Locking**: cột `version` trong PostgreSQL, chỉ worker nào match version mới update được
- **Lua Atomic Pop**: `ZRANGEBYSCORE` + `ZREM` trong một transaction Redis, không có race condition
- **Sharding RabbitMQ**: routing key `tasks.user.{user_id}` phân tán tải theo user
- **Dead Letter Queue**: tin nhắn lỗi đưa vào `tasks.dead_letter` để kiểm tra
- **Cassandra TTL**: log tự xoá sau 90 ngày (`default_time_to_live = 7776000`)
- **Retry**: tác vụ thất bại tự retry tối đa `max_retries` lần
