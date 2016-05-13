/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.andes.transports.mqtt.internal;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.config.MqttTransportProperties;
import org.wso2.carbon.andes.transports.server.BrokerException;
import org.wso2.carbon.andes.transports.server.Server;
import org.wso2.carbon.kernel.transports.CarbonTransport;

/**
 * Will be registered via OSGI to listen to MQTT traffic
 */
public class MqttTransport extends CarbonTransport {

    /**
     * Holds the running instance of the MQTT service
     */
    private Server server = null;

    /**
     * Holds the configuration which provides information related to bootstrapping the transport
     */
    private MqttTransportProperties mqttTransportConfiguration;

    private static final Log log = LogFactory.getLog(MqttTransport.class);


    public MqttTransport(MqttTransportProperties mqttTransportConfiguration, Server server) {
        super(mqttTransportConfiguration.getId());
        this.mqttTransportConfiguration = mqttTransportConfiguration;
        this.server = server;
    }

    /**
     * Stops the transport gracefully
     */
    private void stopTransport() {
        //We stop the server when the bundle is deactivated
        log.info("Stopping MQTT Transport");

        if (null != server) {
            if (log.isDebugEnabled()) {
                log.debug("Stopping MQTT transport " + mqttTransportConfiguration.getId());
            }
            server.stop();

        } else {
            log.error("MQTT server was not initialized properly, hence cannot stop the server");
        }
    }


    /**
     * Starts the transport gracefully
     */
    private void startTransport() {

        log.info("Starting MQTT Server");
        //  server = new MqttServer();
        try {
            server.start(this.mqttTransportConfiguration);
        } catch (BrokerException e) {
            String message = "Error occurred while starting the transport " + mqttTransportConfiguration.getId();
            log.error(message, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void start() {
        log.info("Starting " + mqttTransportConfiguration.getId() + " Transport");
        startTransport();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void stop() {
        stopTransport();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beginMaintenance() {
        log.info("Maintenance mode begins for MQTT transport " + mqttTransportConfiguration.getId());
        stopTransport();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void endMaintenance() {
        log.info("Maintenance complete, restarting the MQTT transport " + mqttTransportConfiguration.getId());
        startTransport();
    }

}
