package vn.anv99.taskscheduler.service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import vn.anv99.taskscheduler.config.SchedulerProperties;
import vn.anv99.taskscheduler.messaging.TaskMessage;
import vn.anv99.taskscheduler.model.Task;
import vn.anv99.taskscheduler.repository.TaskRepository;

@Service
public class SchedulerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerService.class);

    private static final String ATOMIC_POP_SCRIPT = """
            local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
            if #items == 0 then
                return {}
            end
            for _, item in ipairs(items) do
                redis.call('ZREM', KEYS[1], item)
            end
            return items
            """;

    private final RedisTemplate<String, String> redisTemplate;
    private final RabbitTemplate rabbitTemplate;
    private final TaskRepository taskRepository;
    private final SchedulerProperties properties;
    private final DefaultRedisScript<List> redisScript;

    @Autowired
    public SchedulerService(
            RedisTemplate<String, String> redisTemplate,
            RabbitTemplate rabbitTemplate,
            TaskRepository taskRepository,
            SchedulerProperties properties
    ) {
        this.redisTemplate = redisTemplate;
        this.rabbitTemplate = rabbitTemplate;
        this.taskRepository = taskRepository;
        this.properties = properties;
        this.redisScript = new DefaultRedisScript<>(ATOMIC_POP_SCRIPT, List.class);
    }

    @Scheduled(fixedDelayString = "${task-scheduler.scheduler-poll-ms}")
    public void enqueueDueTasks() {
        List<String> rawIds = popDueTaskIds();
        if (rawIds.isEmpty()) {
            return;
        }

        List<UUID> ids = rawIds.stream().map(UUID::fromString).toList();
        List<Task> tasks = taskRepository.findAllById(ids);
        for (Task task : tasks) {
            TaskMessage message = new TaskMessage(
                    task.getId(),
                    task.getUserId(),
                    task.getCronExpression(),
                    task.getWebhookUrl(),
                    task.getPayload(),
                    task.getMaxRetries(),
                    task.getRetryCount()
            );
            String routingKey = "tasks.user." + task.getUserId();
            rabbitTemplate.convertAndSend(properties.exchangeName(), routingKey, message);
            LOGGER.info("Scheduler enqueued task {} for user {}", task.getId(), task.getUserId());
        }
    }

    private List<String> popDueTaskIds() {
        long now = System.currentTimeMillis() / 1000L;
        List result = redisTemplate.execute(
                redisScript,
                List.of(properties.redisZsetKey()),
                Long.toString(now),
                Integer.toString(properties.schedulerBatchSize())
        );
        if (result.isEmpty()) {
            return List.of();
        }
        List<String> ids = new ArrayList<>();
        for (Object value : result) {
            ids.add(value.toString());
        }
        return ids;
    }
}
