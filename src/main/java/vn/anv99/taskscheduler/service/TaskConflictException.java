package vn.anv99.taskscheduler.service;

public class TaskConflictException extends RuntimeException {

    public TaskConflictException(String message) {
        super(message);
    }
}
