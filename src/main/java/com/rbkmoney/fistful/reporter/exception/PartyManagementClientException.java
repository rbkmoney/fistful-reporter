package com.rbkmoney.fistful.reporter.exception;

public class PartyManagementClientException extends RuntimeException {

    public PartyManagementClientException() {
    }

    public PartyManagementClientException(String message) {
        super(message);
    }

    public PartyManagementClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public PartyManagementClientException(Throwable cause) {
        super(cause);
    }

    public PartyManagementClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
