package vn.anv99.taskscheduler.service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import vn.anv99.taskscheduler.config.SchedulerProperties;
import vn.anv99.taskscheduler.model.Task;
import vn.anv99.taskscheduler.model.TaskStatus;
import vn.anv99.taskscheduler.repository.TaskRepository;

@Service
public class RelayService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelayService.class);

    private final TaskRepository taskRepository;
    private final StringRedisTemplate redisTemplate;
    private final SchedulerProperties properties;

    @Autowired
    public RelayService(TaskRepository taskRepository, StringRedisTemplate redisTemplate, SchedulerProperties properties) {
        this.taskRepository = taskRepository;
        this.redisTemplate = redisTemplate;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${task-scheduler.relay-interval-seconds}000")
    @Transactional
    public void relayDueTasks() {
        Instant deadline = Instant.now().plus(properties.relayLookaheadMinutes(), ChronoUnit.MINUTES);
        List<Task> tasks = taskRepository.findByStatusAndNextRunAtLessThanEqual(TaskStatus.PENDING, deadline);
        if (tasks.isEmpty()) {
            return;
        }

        for (Task task : tasks) {
            redisTemplate.opsForZSet().add(
                    properties.redisZsetKey(),
                    task.getId().toString(),
                    task.getNextRunAt().getEpochSecond()
            );
            task.setStatus(TaskStatus.SCHEDULED);
        }
        taskRepository.saveAll(tasks);
        LOGGER.info("Relay pushed {} tasks to Redis", tasks.size());
    }
}
