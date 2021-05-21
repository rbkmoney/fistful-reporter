package com.rbkmoney.fistful.reporter.exception;

public class SinkEventNotFoundException extends RuntimeException {

    public SinkEventNotFoundException() {
    }

    public SinkEventNotFoundException(String message) {
        super(message);
    }

    public SinkEventNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public SinkEventNotFoundException(Throwable cause) {
        super(cause);
    }

    public SinkEventNotFoundException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
