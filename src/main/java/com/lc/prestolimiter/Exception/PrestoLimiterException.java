package com.lc.prestolimiter.Exception;


public class PrestoLimiterException extends RuntimeException {

    public PrestoLimiterException() {
        super();
    }

    public PrestoLimiterException(String message) {
        super(message);
    }

    public PrestoLimiterException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
