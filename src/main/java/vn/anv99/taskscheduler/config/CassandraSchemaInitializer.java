package vn.anv99.taskscheduler.config;

import com.datastax.oss.driver.api.core.CqlSession;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CassandraSchemaInitializer {

    private final CqlSession session;
    private final SchedulerProperties properties;

    @Autowired
    public CassandraSchemaInitializer(CqlSession session, SchedulerProperties properties) {
        this.session = session;
        this.properties = properties;
    }

    @PostConstruct
    void initialize() {
        String keyspace = properties.cassandraKeyspace();
        session.execute("""
                CREATE KEYSPACE IF NOT EXISTS %s
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                """.formatted(keyspace));
        session.execute("""
                CREATE TABLE IF NOT EXISTS %s.task_execution_logs (
                    task_id text,
                    exec_id uuid,
                    started_at timestamp,
                    finished_at timestamp,
                    status text,
                    result text,
                    error_msg text,
                    worker_id text,
                    PRIMARY KEY (task_id, exec_id)
                ) WITH CLUSTERING ORDER BY (exec_id DESC)
                  AND default_time_to_live = 7776000
                """.formatted(keyspace));
    }
}
