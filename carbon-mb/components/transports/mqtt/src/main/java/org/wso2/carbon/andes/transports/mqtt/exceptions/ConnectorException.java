package org.wso2.carbon.andes.transports.mqtt.exceptions;

/**
 * Exception which will thrown from the connector
 */
public class ConnectorException extends Exception {
    public ConnectorException() {
    }

    public ConnectorException(String message) {
        super(message);
    }

    public ConnectorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectorException(Throwable cause) {
        super(cause);
    }

}
