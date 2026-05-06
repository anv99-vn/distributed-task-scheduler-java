package vn.anv99.taskscheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import vn.anv99.taskscheduler.api.TaskController;
import vn.anv99.taskscheduler.dto.TaskCreateRequest;
import vn.anv99.taskscheduler.dto.TaskListResponse;
import vn.anv99.taskscheduler.dto.TaskResponse;
import vn.anv99.taskscheduler.model.TaskStatus;
import vn.anv99.taskscheduler.service.TaskService;

@ExtendWith(MockitoExtension.class)
class TaskControllerTest {

    private MockMvc mockMvc;

    @Mock
    private TaskService taskService;

    @InjectMocks
    private TaskController taskController;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

    private UUID taskId;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(taskController).build();
        taskId = UUID.randomUUID();
    }

    @Test
    void createTask_Success() throws Exception {
        TaskCreateRequest request = new TaskCreateRequest();
        request.setUserId("test-user");
        request.setName("Test Task");
        request.setWebhookUrl("http://localhost:8080/webhook");
        request.setExecuteAt(Instant.now().plusSeconds(3600));

        TaskResponse response = new TaskResponse(
                taskId,
                "test-user",
                "Test Task",
                null,
                null,
                Instant.now().plusSeconds(3600),
                null,
                "http://localhost:8080/webhook",
                JsonNodeFactory.instance.objectNode(),
                TaskStatus.PENDING,
                0,
                3,
                0,
                Instant.now(),
                Instant.now()
        );

        when(taskService.create(any(TaskCreateRequest.class))).thenReturn(response);

        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(taskId.toString()))
                .andExpect(jsonPath("$.userId").value("test-user"))
                .andExpect(jsonPath("$.name").value("Test Task"))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void getTask_Success() throws Exception {
        TaskResponse response = new TaskResponse(
                taskId,
                "test-user",
                "Test Task",
                null,
                null,
                Instant.now().plusSeconds(3600),
                null,
                "http://localhost:8080/webhook",
                JsonNodeFactory.instance.objectNode(),
                TaskStatus.PENDING,
                0,
                3,
                0,
                Instant.now(),
                Instant.now()
        );

        when(taskService.get(taskId)).thenReturn(response);

        mockMvc.perform(get("/api/v1/tasks/{taskId}", taskId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(taskId.toString()))
                .andExpect(jsonPath("$.name").value("Test Task"));
    }

    @Test
    void cancelTask_Success() throws Exception {
        TaskResponse response = new TaskResponse(
                taskId,
                "test-user",
                "Test Task",
                null,
                null,
                Instant.now().plusSeconds(3600),
                null,
                "http://localhost:8080/webhook",
                JsonNodeFactory.instance.objectNode(),
                TaskStatus.CANCELLED,
                0,
                3,
                0,
                Instant.now(),
                Instant.now()
        );

        when(taskService.cancel(taskId)).thenReturn(response);

        mockMvc.perform(delete("/api/v1/tasks/{taskId}", taskId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(taskId.toString()))
                .andExpect(jsonPath("$.status").value("CANCELLED"));
    }

    @Test
    void listTasks_Success() throws Exception {
        TaskResponse task1 = new TaskResponse(
                UUID.randomUUID(),
                "test-user",
                "Task 1",
                null,
                null,
                Instant.now().plusSeconds(3600),
                null,
                "http://localhost:8080/webhook1",
                JsonNodeFactory.instance.objectNode(),
                TaskStatus.PENDING,
                0,
                3,
                0,
                Instant.now(),
                Instant.now()
        );

        TaskResponse task2 = new TaskResponse(
                UUID.randomUUID(),
                "test-user",
                "Task 2",
                null,
                null,
                Instant.now().plusSeconds(7200),
                null,
                "http://localhost:8080/webhook2",
                JsonNodeFactory.instance.objectNode(),
                TaskStatus.SCHEDULED,
                0,
                3,
                0,
                Instant.now(),
                Instant.now()
        );

        TaskListResponse response = new TaskListResponse(List.of(task1, task2), 2L, 1, 20);

        when(taskService.list(eq("test-user"), eq(1), eq(20))).thenReturn(response);

        mockMvc.perform(get("/api/v1/tasks")
                        .param("user_id", "test-user")
                        .param("page", "1")
                        .param("page_size", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tasks").isArray())
                .andExpect(jsonPath("$.tasks.length()").value(2))
                .andExpect(jsonPath("$.total").value(2))
                .andExpect(jsonPath("$.page").value(1))
                .andExpect(jsonPath("$.pageSize").value(20));
    }

    @Test
    void listTasks_EmptyList() throws Exception {
        TaskListResponse response = new TaskListResponse(List.of(), 0L, 1, 20);

        when(taskService.list(eq("empty-user"), eq(1), eq(20))).thenReturn(response);

        mockMvc.perform(get("/api/v1/tasks")
                        .param("user_id", "empty-user"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tasks").isArray())
                .andExpect(jsonPath("$.tasks.length()").value(0))
                .andExpect(jsonPath("$.total").value(0));
    }

    @Test
    void listTasks_WithoutUserId() throws Exception {
        TaskResponse task = new TaskResponse(
                UUID.randomUUID(),
                "user1",
                "Task without filter",
                null,
                null,
                Instant.now().plusSeconds(3600),
                null,
                "http://localhost:8080/webhook",
                JsonNodeFactory.instance.objectNode(),
                TaskStatus.PENDING,
                0,
                3,
                0,
                Instant.now(),
                Instant.now()
        );

        TaskListResponse response = new TaskListResponse(List.of(task), 1L, 1, 20);

        when(taskService.list(eq(null), eq(1), eq(20))).thenReturn(response);

        mockMvc.perform(get("/api/v1/tasks"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tasks").isArray())
                .andExpect(jsonPath("$.tasks.length()").value(1));
    }

    @Test
    void listTasks_WithPagination() throws Exception {
        TaskListResponse response = new TaskListResponse(List.of(), 100L, 2, 10);

        when(taskService.list(eq("test-user"), eq(2), eq(10))).thenReturn(response);

        mockMvc.perform(get("/api/v1/tasks")
                        .param("user_id", "test-user")
                        .param("page", "2")
                        .param("page_size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.page").value(2))
                .andExpect(jsonPath("$.pageSize").value(10))
                .andExpect(jsonPath("$.total").value(100));
    }

    @Test
    void createTaskThenList_ContainsCreatedTask() throws Exception {
        // 1. Chuẩn bị request tạo task
        TaskCreateRequest createRequest = new TaskCreateRequest();
        createRequest.setUserId("test-user");
        createRequest.setName("New Task");
        createRequest.setWebhookUrl("http://localhost:8080/webhook");
        createRequest.setExecuteAt(Instant.now().plusSeconds(3600));

        // 2. Tạo response task vừa được tạo
        UUID createdTaskId = UUID.randomUUID();
        TaskResponse createdTask = new TaskResponse(
                createdTaskId,
                "test-user",
                "New Task",
                null,
                null,
                Instant.now().plusSeconds(3600),
                null,
                "http://localhost:8080/webhook",
                JsonNodeFactory.instance.objectNode(),
                TaskStatus.PENDING,
                0,
                3,
                0,
                Instant.now(),
                Instant.now()
        );

        // 3. Mock service tạo task
        when(taskService.create(any(TaskCreateRequest.class))).thenReturn(createdTask);

        // 4. Mock service lấy danh sách trả về task vừa tạo
        when(taskService.list(eq("test-user"), eq(1), eq(20)))
                .thenReturn(new TaskListResponse(List.of(createdTask), 1L, 1, 20));

        // 5. Gọi API tạo task
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(createRequest)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(createdTaskId.toString()));

        // 6. Gọi API lấy danh sách và kiểm tra
        mockMvc.perform(get("/api/v1/tasks")
                        .param("user_id", "test-user")
                        .param("page", "1")
                        .param("page_size", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tasks").isArray())
                .andExpect(jsonPath("$.tasks.length()").value(1))
                .andExpect(jsonPath("$.tasks[0].id").value(createdTaskId.toString()))
                .andExpect(jsonPath("$.tasks[0].name").value("New Task"));
    }
}
