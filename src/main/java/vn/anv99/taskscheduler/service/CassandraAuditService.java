package vn.anv99.taskscheduler.service;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import vn.anv99.taskscheduler.config.SchedulerProperties;
import vn.anv99.taskscheduler.model.TaskStatus;

@Service
public class CassandraAuditService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraAuditService.class);

    private final CqlSession session;
    private final SchedulerProperties properties;
    private final String workerId;

    @Autowired
    public CassandraAuditService(CqlSession session, SchedulerProperties properties) {
        this.session = session;
        this.properties = properties;
        this.workerId = resolveWorkerId();
    }

    public void logExecution(
            UUID taskId,
            UUID executionId,
            Instant startedAt,
            Instant finishedAt,
            TaskStatus status,
            String result,
            String error
    ) {
        try {
            session.execute(
                    """
                    INSERT INTO %s.task_execution_logs
                        (task_id, exec_id, started_at, finished_at, status, result, error_msg, worker_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """.formatted(properties.cassandraKeyspace()),
                    taskId.toString(),
                    executionId,
                    startedAt,
                    finishedAt,
                    status.name(),
                    truncate(result, 4096),
                    truncate(error, 2048),
                    workerId
            );
        } catch (RuntimeException ex) {
            LOGGER.error("Failed to write Cassandra audit log for task {}", taskId, ex);
        }
    }

    private String resolveWorkerId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            return "unknown";
        }
    }

    private String truncate(String value, int maxLength) {
        if (value == null) {
            return "";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
