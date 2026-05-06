package vn.anv99.taskscheduler.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

public record TaskMessage(
        UUID taskId,
        String userId,
        String cronExpression,
        String webhookUrl,
        JsonNode payload,
        int maxRetries,
        int retryCount
) {
}
