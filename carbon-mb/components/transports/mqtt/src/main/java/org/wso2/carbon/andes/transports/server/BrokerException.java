package org.wso2.carbon.andes.transports.server;

/**
 * Exception which will thrown from the broker
 */
public class BrokerException extends Exception {
    public BrokerException() {
    }

    public BrokerException(String message) {
        super(message);
    }

    public BrokerException(String message, Throwable cause) {
        super(message, cause);
    }

    public BrokerException(Throwable cause) {
        super(cause);
    }
}
