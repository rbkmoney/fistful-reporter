package com.rbkmoney.fistful.reporter.exception;

public class FileStorageClientException extends StorageException {

    public FileStorageClientException() {
    }

    public FileStorageClientException(String message) {
        super(message);
    }

    public FileStorageClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileStorageClientException(Throwable cause) {
        super(cause);
    }

    public FileStorageClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
