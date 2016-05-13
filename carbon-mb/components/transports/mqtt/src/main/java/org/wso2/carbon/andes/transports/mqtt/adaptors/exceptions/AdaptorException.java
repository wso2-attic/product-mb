package org.wso2.carbon.andes.transports.mqtt.adaptors.exceptions;

/**
 * Exception which will thrown from the connector
 */
public class AdaptorException extends Exception {
    public AdaptorException() {
    }

    public AdaptorException(String message) {
        super(message);
    }

    public AdaptorException(String message, Throwable cause) {
        super(message, cause);
    }

    public AdaptorException(Throwable cause) {
        super(cause);
    }

}
