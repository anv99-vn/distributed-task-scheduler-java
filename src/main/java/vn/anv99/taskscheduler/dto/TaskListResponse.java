package vn.anv99.taskscheduler.dto;

import java.util.List;

public record TaskListResponse(
        List<TaskResponse> tasks,
        long total,
        int page,
        int pageSize
) {
}
