# Distributed Task Scheduler

Distributed task scheduler rewritten in Java Spring Boot.

## Architecture

```text
Client -> Spring Boot REST API
             |
             v
        PostgreSQL (source of truth)
             |
             +-> Redis ZSET for tasks due inside the lookahead window
             |
        Relay job: PostgreSQL -> Redis
             |
        Scheduler job: Redis Lua atomic pop -> RabbitMQ topic exchange
             |
        Worker listener: RabbitMQ -> webhook -> PostgreSQL + Cassandra audit log
```

## Stack

- Java 17
- Spring Boot 3.3
- PostgreSQL
- Redis ZSET with Lua atomic pop
- RabbitMQ topic exchange and dead-letter queue
- Cassandra audit log table with 90-day TTL

## Run Infrastructure

```bash
docker compose up -d
```

RabbitMQ management UI: `http://localhost:15672` (`admin` / `admin`).

## Run Application

Install Maven, then run:

```bash
mvn spring-boot:run
```

The API listens on `http://localhost:8000`.

Swagger UI is available at `http://localhost:8000/docs`.

## Configuration

The defaults match `docker-compose.yml`. Override with environment variables when needed:

```env
POSTGRES_JDBC_URL=jdbc:postgresql://localhost:5432/taskscheduler
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
REDIS_URL=redis://localhost:6379/0
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USERNAME=admin
RABBITMQ_PASSWORD=admin
CASSANDRA_CONTACT_POINTS=localhost
CASSANDRA_PORT=9042
CASSANDRA_LOCAL_DATACENTER=dc1
CASSANDRA_KEYSPACE=task_scheduler

RELAY_INTERVAL_SECONDS=300
RELAY_LOOKAHEAD_MINUTES=15
SCHEDULER_POLL_MS=100
SCHEDULER_BATCH_SIZE=50
RATE_LIMIT_PER_MINUTE=60
```

## API

Create a one-time task:

```bash
curl -X POST http://localhost:8000/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user123",
    "name": "Send report",
    "execute_at": "2026-05-06T15:00:00Z",
    "webhook_url": "https://webhook.site/your-id",
    "payload": {"report_type": "daily"}
  }'
```

Create a recurring task with a 5-field cron expression:

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

List tasks:

```bash
curl "http://localhost:8000/api/v1/tasks?user_id=user123&page=1&page_size=20"
```

Get task:

```bash
curl http://localhost:8000/api/v1/tasks/{taskId}
```

Cancel task:

```bash
curl -X DELETE http://localhost:8000/api/v1/tasks/{taskId}
```

## Main Java Components

| Component | Responsibility |
| --- | --- |
| `TaskController` | REST API under `/api/v1/tasks` |
| `TaskService` | Validate input, write PostgreSQL, buffer near-due tasks into Redis |
| `RelayService` | Scheduled PostgreSQL scan to Redis ZSET |
| `SchedulerService` | Lua atomic pop from Redis and publish to RabbitMQ |
| `WorkerService` | Consume RabbitMQ messages, execute webhook, update task state |
| `CassandraAuditService` | Write execution logs to Cassandra |
| `RateLimitInterceptor` | Simple per-IP API limit |
