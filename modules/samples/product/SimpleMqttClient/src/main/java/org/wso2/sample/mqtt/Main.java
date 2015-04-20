/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.sample.mqtt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

/**
 * This samples demonstrates how to write a simple MQTT client to send/receive message via MQTT in WSO2 Message Broker.
 */
public class Main {

    private static final Log log = LogFactory.getLog(Main.class);

    // Java temporary directory location
    private static final String JAVA_TMP_DIR = System.getProperty("java.io.tmpdir");

    // The MQTT broker URL
    private static final String brokerURL = "tcp://localhost:1883";

    /**
     * The main method which runs the sample.
     *
     * @param args Commandline arguments
     */
    public static void main(String[] args) {
        String subscriberClientId = "subscriber";
        String publisherClientId = "publisher";
        String topic = "simpleTopic";
        boolean retained = false;

        log.info("Running sample");
        byte[] payload = "hello".getBytes();

        try {
            // Creating mqtt subscriber client
            MqttClient mqttSubscriberClient = getNewMqttClient(subscriberClientId);

            // Creating mqtt publisher client
            MqttClient mqttPublisherClient = getNewMqttClient(publisherClientId);

            // Subscribing to mqtt topic "simpleTopic"
            mqttSubscriberClient.subscribe(topic, QualityOfService.LEAST_ONCE.getValue());

            // Publishing to mqtt topic "simpleTopic"
            mqttPublisherClient.publish(topic, payload, QualityOfService.LEAST_ONCE.getValue(), retained);

            mqttPublisherClient.disconnect();
            mqttSubscriberClient.disconnect();

            log.info("Clients Disconnected!");
        } catch (MqttException e) {
            log.error("Error running the sample", e);
        }


    }

    /**
     * Crate a new MQTT client and connect it to the server.
     *
     * @param clientId The unique mqtt client Id
     * @return Connected MQTT client
     * @throws MqttException
     */
    private static MqttClient getNewMqttClient(String clientId) throws MqttException {
        //Store messages until server fetches them
        MqttDefaultFilePersistence dataStore = new MqttDefaultFilePersistence(JAVA_TMP_DIR + "/" + clientId);

        MqttClient mqttClient = new MqttClient(brokerURL, clientId, dataStore);
        SimpleMQTTCallback callback = new SimpleMQTTCallback();
        mqttClient.setCallback(callback);

        MqttConnectOptions connectOptions = new MqttConnectOptions();

        connectOptions.setUserName("admin");
        connectOptions.setPassword("admin".toCharArray());
        connectOptions.setCleanSession(true);
        mqttClient.connect(connectOptions);


        return mqttClient;
    }

}
