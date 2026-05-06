package vn.anv99.taskscheduler.service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import vn.anv99.taskscheduler.config.SchedulerProperties;
import vn.anv99.taskscheduler.messaging.TaskMessage;
import vn.anv99.taskscheduler.model.Task;
import vn.anv99.taskscheduler.model.TaskStatus;
import vn.anv99.taskscheduler.repository.TaskRepository;

@Service
public class WorkerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerService.class);

    private final TaskRepository taskRepository;
    private final CassandraAuditService auditService;
    private final CronService cronService;
    private final StringRedisTemplate redisTemplate;
    private final SchedulerProperties properties;
    private final RestClient restClient;
    private final TransactionTemplate transactionTemplate;

    @Autowired
    public WorkerService(
            TaskRepository taskRepository,
            CassandraAuditService auditService,
            CronService cronService,
            StringRedisTemplate redisTemplate,
            SchedulerProperties properties,
            RestClient.Builder restClientBuilder,
            TransactionTemplate transactionTemplate
    ) {
        this.taskRepository = taskRepository;
        this.auditService = auditService;
        this.cronService = cronService;
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.restClient = restClientBuilder.build();
        this.transactionTemplate = transactionTemplate;
    }

    @RabbitListener(queues = "${task-scheduler.worker-queue-name}")
    public void handle(TaskMessage message) {
        UUID taskId = message.taskId();
        UUID executionId = UUID.randomUUID();
        Instant startedAt = Instant.now();

        Task acquired = transactionTemplate.execute(status -> acquire(taskId));
        if (acquired == null) {
            return;
        }

        TaskStatus executionStatus = TaskStatus.SUCCESS;
        String result = "";
        String error = "";
        try {
            ResponseEntity<String> response = restClient.post()
                    .uri(message.webhookUrl())
                    .body(message.payload())
                    .retrieve()
                    .toEntity(String.class);
            result = response.getBody() == null ? "" : response.getBody();
            LOGGER.info("Task {} succeeded with HTTP {}", taskId, response.getStatusCode().value());
        } catch (RestClientException ex) {
            executionStatus = TaskStatus.FAILED;
            error = ex.getMessage();
            LOGGER.error("Task {} failed: {}", taskId, error);
        }

        Instant finishedAt = Instant.now();
        TaskStatus finalExecutionStatus = executionStatus;
        transactionTemplate.executeWithoutResult(status -> finish(taskId, message, finalExecutionStatus, finishedAt));
        auditService.logExecution(taskId, executionId, startedAt, finishedAt, executionStatus, result, error);
    }

    private Task acquire(UUID taskId) {
        Task task = taskRepository.findById(taskId).orElse(null);
        if (task == null) {
            LOGGER.warn("Task {} not found, dropping message", taskId);
            return null;
        }
        if (!List.of(TaskStatus.SCHEDULED, TaskStatus.PENDING).contains(task.getStatus())) {
            LOGGER.info("Task {} already in status {}, skipping", taskId, task.getStatus());
            return null;
        }
        int updated = taskRepository.updateStatusOptimistic(
                taskId,
                task.getStatus(),
                TaskStatus.RUNNING,
                task.getVersion(),
                Instant.now()
        );
        if (updated == 0) {
            LOGGER.warn("Optimistic lock failed for task {}", taskId);
            return null;
        }
        task.setStatus(TaskStatus.RUNNING);
        task.setVersion(task.getVersion() + 1);
        return task;
    }

    private void finish(UUID taskId, TaskMessage message, TaskStatus executionStatus, Instant finishedAt) {
        Task task = taskRepository.findById(taskId).orElse(null);
        if (task == null || task.getStatus() != TaskStatus.RUNNING) {
            LOGGER.warn("Task {} is no longer RUNNING, final update skipped", taskId);
            return;
        }

        if (executionStatus == TaskStatus.SUCCESS && message.cronExpression() != null) {
            task.setNextRunAt(cronService.nextRun(message.cronExpression(), finishedAt));
            task.setStatus(TaskStatus.PENDING);
            task.setRetryCount(0);
        } else if (executionStatus == TaskStatus.FAILED && task.getRetryCount() < task.getMaxRetries()) {
            task.setRetryCount(task.getRetryCount() + 1);
            task.setNextRunAt(Instant.now());
            task.setStatus(TaskStatus.PENDING);
        } else {
            task.setStatus(executionStatus);
        }

        Task saved = taskRepository.saveAndFlush(task);
        scheduleIfDue(saved);
    }

    private void scheduleIfDue(Task task) {
        if (task.getStatus() != TaskStatus.PENDING || task.getNextRunAt() == null) {
            return;
        }
        Instant lookahead = Instant.now().plus(properties.relayLookaheadMinutes(), ChronoUnit.MINUTES);
        if (task.getNextRunAt().isAfter(lookahead)) {
            return;
        }
        redisTemplate.opsForZSet().add(
                properties.redisZsetKey(),
                task.getId().toString(),
                task.getNextRunAt().getEpochSecond()
        );
        task.setStatus(TaskStatus.SCHEDULED);
        taskRepository.save(task);
    }
}
