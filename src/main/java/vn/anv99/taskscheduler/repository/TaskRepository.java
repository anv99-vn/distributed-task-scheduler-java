package vn.anv99.taskscheduler.repository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import vn.anv99.taskscheduler.model.Task;
import vn.anv99.taskscheduler.model.TaskStatus;

public interface TaskRepository extends JpaRepository<Task, UUID> {

    Page<Task> findByUserId(String userId, Pageable pageable);

    List<Task> findByStatusAndNextRunAtLessThanEqual(TaskStatus status, Instant deadline);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Task t
               set t.status = :toStatus,
                   t.updatedAt = :updatedAt
             where t.id = :id
               and t.status in :fromStatuses
            """)
    int cancelIfStatusIn(
            @Param("id") UUID id,
            @Param("fromStatuses") Collection<TaskStatus> fromStatuses,
            @Param("toStatus") TaskStatus toStatus,
            @Param("updatedAt") Instant updatedAt
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Task t
               set t.status = :toStatus,
                   t.version = t.version + 1,
                   t.updatedAt = :updatedAt
             where t.id = :id
               and t.status = :fromStatus
               and t.version = :version
            """)
    int updateStatusOptimistic(
            @Param("id") UUID id,
            @Param("fromStatus") TaskStatus fromStatus,
            @Param("toStatus") TaskStatus toStatus,
            @Param("version") int version,
            @Param("updatedAt") Instant updatedAt
    );
}
