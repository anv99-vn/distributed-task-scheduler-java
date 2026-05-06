package vn.anv99.taskscheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;
import vn.anv99.taskscheduler.config.SchedulerProperties;
import vn.anv99.taskscheduler.model.TaskStatus;
import vn.anv99.taskscheduler.repository.TaskRepository;
import vn.anv99.taskscheduler.service.CassandraAuditService;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:postgresql://localhost:5432/taskscheduler",
        "spring.datasource.username=postgres",
        "spring.datasource.password=postgres",
        "spring.data.redis.url=redis://localhost:6379/0",
        "spring.rabbitmq.host=localhost",
        "spring.rabbitmq.port=5672",
        "spring.rabbitmq.username=admin",
        "spring.rabbitmq.password=admin",
        "spring.cassandra.contact-points=localhost",
        "spring.cassandra.port=9042",
        "spring.cassandra.local-datacenter=datacenter1",
        "task-scheduler.relay-interval-seconds=1",
        "task-scheduler.scheduler-poll-ms=100",
        "task-scheduler.relay-lookahead-minutes=1",
        "task-scheduler.rate-limit-per-minute=3"
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaskApiEndToEndTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private CqlSession cqlSession;

    @Autowired
    private SchedulerProperties properties;

    @Autowired
    private CassandraAuditService cassandraAuditService;

    private final List<JsonNode> receivedWebhooks = new CopyOnWriteArrayList<>();
    private final AtomicInteger clientIpCounter = new AtomicInteger(10);

    private HttpServer webhookServer;
    private String webhookUrl;

    @BeforeAll
    void startWebhookServer() throws IOException {
        webhookServer = HttpServer.create(new InetSocketAddress(0), 0);
        webhookServer.createContext("/webhook", this::handleWebhook);
        webhookServer.start();
        webhookUrl = "http://localhost:%d/webhook".formatted(webhookServer.getAddress().getPort());
    }

    @AfterAll
    void stopWebhookServer() {
        if (webhookServer != null) {
            webhookServer.stop(0);
        }
    }

    @BeforeEach
    void cleanState() {
        receivedWebhooks.clear();
        clientIpCounter.set(10);
        redisTemplate.delete(properties.redisZsetKey());
        taskRepository.deleteAllInBatch();
        cqlSession.execute("TRUNCATE %s.task_execution_logs".formatted(properties.cassandraKeyspace()));
    }

    @Test
    void endToEndFlowCoversSchedulingWebhookAuditCancelRateLimitAndValidation() throws Exception {
        assertHealth();
        assertValidationErrors();
        assertNotFound();

        JsonNode cronTask = createTask(Map.of(
                "user_id", "e2e-user",
                "name", "Cron every minute",
                "cron_expression", "* * * * *",
                "webhook_url", webhookUrl,
                "payload", Map.of("type", "cron"),
                "max_retries", 2
        ));
        assertThat(cronTask.path("cron_expression").asText()).isEqualTo("* * * * *");
        assertThat(cronTask.path("next_run_at").isNull()).isFalse();
        assertThat(cancelTask(cronTask.path("id").asText()).path("status").asText()).isEqualTo("CANCELLED");

        JsonNode cancellableTask = createTask(Map.of(
                "user_id", "e2e-user",
                "name", "Cancel me",
                "execute_at", Instant.now().plus(Duration.ofHours(1)).toString(),
                "webhook_url", webhookUrl,
                "payload", Map.of()
        ));
        String cancellableTaskId = cancellableTask.path("id").asText();
        assertThat(cancelTask(cancellableTaskId).path("status").asText()).isEqualTo("CANCELLED");
        ResponseEntity<JsonNode> cancelAgain = restTemplate.exchange(
                "/api/v1/tasks/{taskId}",
                HttpMethod.DELETE,
                null,
                JsonNode.class,
                cancellableTaskId
        );
        assertThat(cancelAgain.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        ResponseEntity<JsonNode> listResponse = exchangeWithUniqueIp(
                "/api/v1/tasks?user_id={userId}&page=1&page_size=20",
                HttpMethod.GET,
                null,
                JsonNode.class,
                "e2e-user"
        );
        assertThat(listResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(listResponse.getBody()).isNotNull();
        assertThat(listResponse.getBody().path("tasks").isArray()).isTrue();
        assertThat(listResponse.getBody().path("total").asLong()).isGreaterThanOrEqualTo(2L);

        JsonNode oneTimeTask = createTask(Map.of(
                "user_id", "e2e-user",
                "name", "Webhook execution",
                "execute_at", Instant.now().plusSeconds(8).toString(),
                "webhook_url", webhookUrl,
                "payload", Map.of("test", "one-time", "request_id", UUID.randomUUID().toString())
        ));

        String taskId = oneTimeTask.path("id").asText();
        assertThat(oneTimeTask.path("status").asText()).isIn("PENDING", "SCHEDULED");

        List<String> statuses = pollStatusesUntilFinished(taskId, Duration.ofSeconds(90));
        assertThat(statuses).contains("SCHEDULED", "RUNNING", "SUCCESS");

        assertThat(receivedWebhooks).hasSize(1);
        JsonNode webhookPayload = receivedWebhooks.get(0);
        assertThat(webhookPayload.path("test").asText()).isEqualTo("one-time");

        JsonNode finalTask = getTask(taskId);
        assertThat(finalTask.path("status").asText()).isEqualTo("SUCCESS");
        assertThat(finalTask.path("retry_count").asInt()).isZero();
        assertThat(finalTask.path("version").asInt()).isGreaterThan(0);

        JsonNode auditLog = pollAuditLog(taskId, Duration.ofSeconds(30));
        assertThat(auditLog.path("status").asText()).isEqualTo("SUCCESS");
        assertThat(auditLog.path("result").asText()).contains("\"ok\":true");

        assertRateLimit();
    }

    @Test
    void cassandraSchemaAndAuditLogBehaviorAreCorrect() {
        var table = cqlSession.execute("""
                SELECT table_name, default_time_to_live
                  FROM system_schema.tables
                 WHERE keyspace_name = ?
                   AND table_name = 'task_execution_logs'
                """, properties.cassandraKeyspace()).one();

        assertThat(table).isNotNull();
        assertThat(table.getString("table_name")).isEqualTo("task_execution_logs");
        assertThat(table.getInt("default_time_to_live")).isEqualTo(7_776_000);

        UUID taskId = UUID.randomUUID();
        UUID executionId = UUID.randomUUID();
        Instant startedAt = Instant.now().minusSeconds(5);
        Instant finishedAt = Instant.now();
        String longResult = "r".repeat(5_000);
        String longError = "e".repeat(3_000);

        cassandraAuditService.logExecution(
                taskId,
                executionId,
                startedAt,
                finishedAt,
                TaskStatus.FAILED,
                longResult,
                longError
        );

        var row = cqlSession.execute(
                """
                SELECT exec_id, status, result, error_msg, worker_id
                  FROM %s.task_execution_logs
                 WHERE task_id = ?
                   AND exec_id = ?
                """.formatted(properties.cassandraKeyspace()),
                taskId.toString(),
                executionId
        ).one();

        assertThat(row).isNotNull();
        assertThat(row.getUuid("exec_id")).isEqualTo(executionId);
        assertThat(row.getString("status")).isEqualTo("FAILED");
        assertThat(row.getString("result")).hasSize(4_096);
        assertThat(row.getString("error_msg")).hasSize(2_048);
        assertThat(row.getString("worker_id")).isNotBlank();
    }

    private void assertHealth() {
        ResponseEntity<JsonNode> response = restTemplate.getForEntity("/actuator/health", JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().path("status").asText()).isEqualTo("UP");
    }

    private void assertValidationErrors() {
        assertBadRequest(Map.of(
                "user_id", "tester",
                "name", "missing schedule",
                "webhook_url", webhookUrl
        ));
        assertBadRequest(Map.of(
                "user_id", "tester",
                "name", "bad cron",
                "cron_expression", "not-a-cron",
                "webhook_url", webhookUrl
        ));
        assertBadRequest(Map.of(
                "user_id", "tester",
                "name", "bad url",
                "execute_at", Instant.now().plusSeconds(30).toString(),
                "webhook_url", "not-a-url"
        ));
        assertBadRequest(Map.of(
                "user_id", "tester",
                "name", "both schedule types",
                "cron_expression", "* * * * *",
                "execute_at", Instant.now().plusSeconds(30).toString(),
                "webhook_url", webhookUrl
        ));
    }

    private void assertBadRequest(Map<String, Object> request) {
        ResponseEntity<JsonNode> response = exchangeWithUniqueIp(
                "/api/v1/tasks",
                HttpMethod.POST,
                request,
                JsonNode.class
        );
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().path("detail").asText()).isNotBlank();
    }

    private void assertNotFound() {
        ResponseEntity<JsonNode> response = exchangeWithUniqueIp(
                "/api/v1/tasks/{taskId}",
                HttpMethod.GET,
                null,
                JsonNode.class,
                UUID.randomUUID()
        );
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private JsonNode createTask(Map<String, Object> request) {
        ResponseEntity<JsonNode> response = exchangeWithUniqueIp(
                "/api/v1/tasks",
                HttpMethod.POST,
                request,
                JsonNode.class
        );
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().path("id").asText()).isNotBlank();
        return response.getBody();
    }

    private JsonNode cancelTask(String taskId) {
        ResponseEntity<JsonNode> response = exchangeWithUniqueIp(
                "/api/v1/tasks/{taskId}",
                HttpMethod.DELETE,
                null,
                JsonNode.class,
                taskId
        );
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        return response.getBody();
    }

    private JsonNode getTask(String taskId) {
        ResponseEntity<JsonNode> response = exchangeWithUniqueIp(
                "/api/v1/tasks/{taskId}",
                HttpMethod.GET,
                null,
                JsonNode.class,
                taskId
        );
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        return response.getBody();
    }

    private List<String> pollStatusesUntilFinished(String taskId, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        List<String> statuses = new ArrayList<>();
        while (Instant.now().isBefore(deadline)) {
            String status = getTask(taskId).path("status").asText();
            if (statuses.isEmpty() || !statuses.get(statuses.size() - 1).equals(status)) {
                statuses.add(status);
            }
            if ("SUCCESS".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status)) {
                return statuses;
            }
            Thread.sleep(200);
        }
        return statuses;
    }

    private JsonNode pollAuditLog(String taskId, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            var row = cqlSession.execute(
                    "SELECT status, result, error_msg FROM %s.task_execution_logs WHERE task_id = ? LIMIT 1"
                            .formatted(properties.cassandraKeyspace()),
                    taskId
            ).one();
            if (row != null) {
                var node = OBJECT_MAPPER.createObjectNode();
                node.put("status", row.getString("status"));
                node.put("result", row.getString("result"));
                if (row.isNull("error_msg")) {
                    node.putNull("error_msg");
                } else {
                    node.put("error_msg", row.getString("error_msg"));
                }
                return node;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("Timed out waiting for Cassandra audit log for task " + taskId);
    }

    private void assertRateLimit() {
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Forwarded-For", "203.0.113.250");
        HttpEntity<Void> request = new HttpEntity<>(headers);

        ResponseEntity<JsonNode> first = restTemplate.exchange("/api/v1/tasks?page=1&page_size=1", HttpMethod.GET, request, JsonNode.class);
        ResponseEntity<JsonNode> second = restTemplate.exchange("/api/v1/tasks?page=1&page_size=1", HttpMethod.GET, request, JsonNode.class);
        ResponseEntity<JsonNode> third = restTemplate.exchange("/api/v1/tasks?page=1&page_size=1", HttpMethod.GET, request, JsonNode.class);
        ResponseEntity<JsonNode> fourth = restTemplate.exchange("/api/v1/tasks?page=1&page_size=1", HttpMethod.GET, request, JsonNode.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(third.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(fourth.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    private <T> ResponseEntity<T> exchangeWithUniqueIp(
            String url,
            HttpMethod method,
            Object body,
            Class<T> responseType,
            Object... uriVariables
    ) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Forwarded-For", "198.51.100.%d".formatted(clientIpCounter.getAndIncrement()));
        HttpEntity<?> request = new HttpEntity<>(body, headers);
        return restTemplate.exchange(url, method, request, responseType, uriVariables);
    }

    private void handleWebhook(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        try (InputStream body = exchange.getRequestBody()) {
            receivedWebhooks.add(OBJECT_MAPPER.readTree(body));
        }

        try {
            Thread.sleep(1_500);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        byte[] response = "{\"ok\":true}".getBytes();
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(response);
        }
    }
}
