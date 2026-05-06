package vn.anv99.taskscheduler.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import vn.anv99.taskscheduler.config.SchedulerProperties;
import vn.anv99.taskscheduler.dto.TaskCreateRequest;
import vn.anv99.taskscheduler.dto.TaskListResponse;
import vn.anv99.taskscheduler.dto.TaskResponse;
import vn.anv99.taskscheduler.model.Task;
import vn.anv99.taskscheduler.model.TaskStatus;
import vn.anv99.taskscheduler.repository.TaskRepository;

@Service
public class TaskService {

    private final TaskRepository taskRepository;
    private final StringRedisTemplate redisTemplate;
    private final SchedulerProperties properties;
    private final CronService cronService;
    private final ObjectMapper objectMapper;

    @Autowired
    public TaskService(
            TaskRepository taskRepository,
            StringRedisTemplate redisTemplate,
            SchedulerProperties properties,
            CronService cronService,
            ObjectMapper objectMapper
    ) {
        this.taskRepository = taskRepository;
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.cronService = cronService;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public TaskResponse create(TaskCreateRequest request) {
        validateSchedule(request);

        Instant nextRunAt = request.getCronExpression() != null
                ? cronService.nextRun(request.getCronExpression(), Instant.now())
                : request.getExecuteAt();

        Task task = new Task();
        task.setUserId(request.getUserId());
        task.setName(request.getName());
        task.setDescription(request.getDescription());
        task.setCronExpression(request.getCronExpression());
        task.setExecuteAt(request.getExecuteAt());
        task.setNextRunAt(nextRunAt);
        task.setWebhookUrl(request.getWebhookUrl());
        task.setPayload(request.getPayload() == null ? objectMapper.createObjectNode() : request.getPayload());
        task.setMaxRetries(request.getMaxRetries());
        task.setRetryCount(0);
        task.setStatus(TaskStatus.PENDING);

        Task saved = taskRepository.saveAndFlush(task);
        Instant lookahead = Instant.now().plus(properties.relayLookaheadMinutes(), ChronoUnit.MINUTES);
        if (!nextRunAt.isAfter(lookahead)) {
            redisTemplate.opsForZSet().add(properties.redisZsetKey(), saved.getId().toString(), nextRunAt.getEpochSecond());
            saved.setStatus(TaskStatus.SCHEDULED);
            saved = taskRepository.saveAndFlush(saved);
        }

        return TaskResponse.from(saved);
    }

    @Transactional(readOnly = true)
    public TaskResponse get(UUID id) {
        return taskRepository.findById(id)
                .map(TaskResponse::from)
                .orElseThrow(() -> new TaskNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public TaskListResponse list(String userId, int page, int pageSize) {
        PageRequest pageable = PageRequest.of(page - 1, pageSize, Sort.by(Sort.Direction.DESC, "createdAt"));
        Page<Task> result = userId == null || userId.isBlank()
                ? taskRepository.findAll(pageable)
                : taskRepository.findByUserId(userId, pageable);
        List<TaskResponse> tasks = result.getContent().stream().map(TaskResponse::from).toList();
        return new TaskListResponse(tasks, result.getTotalElements(), page, pageSize);
    }

    @Transactional
    public TaskResponse cancel(UUID id) {
        Task task = taskRepository.findById(id).orElseThrow(() -> new TaskNotFoundException(id));
        int updated = taskRepository.cancelIfStatusIn(
                id,
                List.of(TaskStatus.PENDING, TaskStatus.SCHEDULED),
                TaskStatus.CANCELLED,
                Instant.now()
        );
        if (updated == 0) {
            throw new TaskConflictException("Cannot cancel task in status " + task.getStatus());
        }
        redisTemplate.opsForZSet().remove(properties.redisZsetKey(), id.toString());
        return TaskResponse.from(taskRepository.findById(id).orElseThrow(() -> new TaskNotFoundException(id)));
    }

    private void validateSchedule(TaskCreateRequest request) {
        boolean hasCron = request.getCronExpression() != null && !request.getCronExpression().isBlank();
        boolean hasExecuteAt = request.getExecuteAt() != null;
        if (hasCron == hasExecuteAt) {
            throw new InvalidTaskException("Provide exactly one of cronExpression or executeAt");
        }
        if (hasCron && !cronService.isValid(request.getCronExpression())) {
            throw new InvalidTaskException("Invalid cron expression: " + request.getCronExpression());
        }
    }
}
