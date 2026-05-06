package vn.anv99.taskscheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "task-scheduler")
public record SchedulerProperties(
        int relayIntervalSeconds,
        int relayLookaheadMinutes,
        long schedulerPollMs,
        int schedulerBatchSize,
        int rateLimitPerMinute,
        String redisZsetKey,
        String exchangeName,
        String deadLetterExchangeName,
        String deadLetterQueueName,
        String workerQueueName,
        String workerRoutingKey,
        String cassandraKeyspace
) {
}
