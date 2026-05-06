package vn.anv99.taskscheduler.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class RateLimitInterceptor implements HandlerInterceptor {

    private final SchedulerProperties properties;
    private final Map<String, Deque<Long>> requestsByIp = new ConcurrentHashMap<>();

    @Autowired
    public RateLimitInterceptor(SchedulerProperties properties) {
        this.properties = properties;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String ip = clientIp(request);
        long now = Instant.now().getEpochSecond();
        long windowStart = now - 60;
        Deque<Long> requests = requestsByIp.computeIfAbsent(ip, ignored -> new ArrayDeque<>());
        synchronized (requests) {
            while (!requests.isEmpty() && requests.peekFirst() < windowStart) {
                requests.removeFirst();
            }
            if (requests.size() >= properties.rateLimitPerMinute()) {
                response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
                response.setContentType("application/json");
                response.getWriter().write("{\"detail\":\"Rate limit exceeded\"}");
                return false;
            }
            requests.addLast(now);
            return true;
        }
    }

    private String clientIp(HttpServletRequest request) {
        String forwardedFor = request.getHeader("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isBlank()) {
            return forwardedFor.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
