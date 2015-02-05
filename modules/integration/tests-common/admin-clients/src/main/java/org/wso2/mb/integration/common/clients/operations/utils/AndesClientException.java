package org.wso2.mb.integration.common.clients.operations.utils;

/**
 *
 */
public class AndesClientException extends Exception {

    public String errorMessage;

    public AndesClientException() {
    }

    public AndesClientException(String message) {
        super(message);
        errorMessage = message;
    }

    public AndesClientException(String message, Throwable cause) {
        super(message, cause);
        errorMessage = message;
    }

    public AndesClientException(Throwable cause) {
        super(cause);
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}