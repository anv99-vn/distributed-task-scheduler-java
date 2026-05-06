package vn.anv99.taskscheduler.service;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;

@Service
public class CronService {

    public Instant nextRun(String expression, Instant baseTime) {
        CronExpression cron = CronExpression.parse(toSpringCron(expression));
        ZonedDateTime next = cron.next(baseTime.atZone(ZoneOffset.UTC));
        if (next == null) {
            throw new IllegalArgumentException("Cron expression has no next execution");
        }
        return next.toInstant();
    }

    public boolean isValid(String expression) {
        try {
            CronExpression.parse(toSpringCron(expression));
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    private String toSpringCron(String expression) {
        String trimmed = expression == null ? "" : expression.trim().replaceAll("\\s+", " ");
        int fields = trimmed.isBlank() ? 0 : trimmed.split(" ").length;
        if (fields == 5) {
            return "0 " + trimmed;
        }
        if (fields == 6) {
            return trimmed;
        }
        throw new IllegalArgumentException("Cron expression must have 5 or 6 fields");
    }
}
