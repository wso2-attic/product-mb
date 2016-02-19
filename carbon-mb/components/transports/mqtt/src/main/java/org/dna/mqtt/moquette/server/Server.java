package org.dna.mqtt.moquette.server;

import org.dna.mqtt.commons.Constants;
import org.dna.mqtt.moquette.messaging.spi.impl.SimpleMessaging;
import org.dna.mqtt.moquette.server.netty.NettyAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Launch a  configured version of the server.
 *
 * @author andrea
 */
public class Server {

    public static final int DEFAULT_MQTT_PORT = 1833;
    private static final Logger log = LoggerFactory.getLogger(org.dna.mqtt.moquette.server.Server.class);
    //Adding the MB directory file path on which the temp db will be stored
    private static final String DB_STORE_PATH = "/repository/database";
    public static final String STORAGE_FILE_PATH = System.getProperty("carbon.home") + DB_STORE_PATH +
                                                   File.separator + "mqtt_store.hawtdb";

    private ServerAcceptor acceptor;
    SimpleMessaging messaging;

    public void startServer() throws IOException {
        if (AndesConfigurationManager.<Boolean>readValue(AndesConfiguration.TRANSPORTS_MQTT_ENABLED)) {
            Properties configProps = loadConfigurations();
            serverInit(configProps);
        } else {
            log.warn("MQTT Transport is disabled as per configuration.");
        }
    }

    /**
     * Load configurations related to MQTT from Andes configuration files.
     *
     * @return Property collection
     * @throws org.wso2.andes.kernel.AndesException
     */
    private Properties loadConfigurations() {

        Properties mqttProperties = new Properties();

        mqttProperties.setProperty(Constants.PORT_PROPERTY_NAME, AndesConfigurationManager.
                readValue(AndesConfiguration.TRANSPORTS_MQTT_DEFAULT_CONNECTION_PORT).toString());

        mqttProperties.setProperty(Constants.SSL_PORT_PROPERTY_NAME, AndesConfigurationManager.
                readValue(AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_PORT).toString());

        mqttProperties.setProperty(Constants.SSL_CONNECTION_ENABLED, AndesConfigurationManager.
                readValue(AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_ENABLED).toString());

        mqttProperties.setProperty(Constants.DEFAULT_CONNECTION_ENABLED, AndesConfigurationManager.
                readValue(AndesConfiguration.TRANSPORTS_MQTT_DEFAULT_CONNECTION_ENABLED).toString());

        mqttProperties.setProperty(Constants.HOST_PROPERTY_NAME, AndesConfigurationManager.
                readValue(AndesConfiguration.TRANSPORTS_MQTT_BIND_ADDRESS).toString());

        return mqttProperties;
    }

    private void serverInit(Properties configProps) throws IOException {
        messaging = SimpleMessaging.getInstance();
        messaging.init(configProps);

        acceptor = new NettyAcceptor();
        acceptor.initialize(messaging, configProps);
    }

    public void stopServer() {
        log.info("MQTT Server is stopping...");
        messaging.stop();
        acceptor.close();
        log.info("MQTT Server has stopped.");
    }
}
