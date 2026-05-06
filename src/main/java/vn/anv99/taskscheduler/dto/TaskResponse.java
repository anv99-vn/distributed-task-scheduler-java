package vn.anv99.taskscheduler.dto;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;
import vn.anv99.taskscheduler.model.Task;
import vn.anv99.taskscheduler.model.TaskStatus;

public record TaskResponse(
        UUID id,
        String userId,
        String name,
        String description,
        String cronExpression,
        Instant executeAt,
        Instant nextRunAt,
        String webhookUrl,
        JsonNode payload,
        TaskStatus status,
        int version,
        int maxRetries,
        int retryCount,
        Instant createdAt,
        Instant updatedAt
) {
    public static TaskResponse from(Task task) {
        return new TaskResponse(
                task.getId(),
                task.getUserId(),
                task.getName(),
                task.getDescription(),
                task.getCronExpression(),
                task.getExecuteAt(),
                task.getNextRunAt(),
                task.getWebhookUrl(),
                task.getPayload(),
                task.getStatus(),
                task.getVersion(),
                task.getMaxRetries(),
                task.getRetryCount(),
                task.getCreatedAt(),
                task.getUpdatedAt()
        );
    }
}
