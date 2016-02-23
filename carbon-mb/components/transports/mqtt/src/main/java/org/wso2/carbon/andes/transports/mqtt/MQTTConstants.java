package org.wso2.carbon.andes.transports.mqtt;

/**
 * Defines attributes related to MQTT
 */
public class MqttConstants {
    /**
     * Defines the duration of a connection
     */
    public static final int DEFAULT_CONNECT_TIMEOUT = 10;

    /**
     * Duration a specific channel could idle without reading from it
     * '0' means it will disable the timeout
     */
    public static final int READER_IDLE_TIME = 0;

    /**
     * Duration a specific channel could idle without writing from it
     * '0' means it will disable the timeout
     */
    public static final int WRITER_IDLE_TIME = 0;

    /**
     * Holds the name of the property the keep alive session attribute will be kept
     */
    public static final String KEEP_ALIVE_PROPERTY_NAME = "keep-alive";

    /**
     * Holds the property name of the client identifier
     */
    public static final String CLIENT_ID_PROPERTY_NAME = "client-id";

    /**
     * Holds the property name of the clean session attribute
     */
    public static final String SESSION_DURABILITY_PROPERTY_NAME = "isCleanSession";

    /**
     * This will be a utility class, hence we do not allow a public constructor
     */
    private MqttConstants() {
    }
}
