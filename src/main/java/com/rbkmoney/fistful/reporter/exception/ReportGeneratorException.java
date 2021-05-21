package com.rbkmoney.fistful.reporter.exception;

public class ReportGeneratorException extends RuntimeException {

    public ReportGeneratorException() {
    }

    public ReportGeneratorException(String message) {
        super(message);
    }

    public ReportGeneratorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReportGeneratorException(Throwable cause) {
        super(cause);
    }

    public ReportGeneratorException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
