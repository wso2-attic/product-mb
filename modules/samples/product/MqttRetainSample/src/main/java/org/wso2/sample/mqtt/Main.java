/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.sample.mqtt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * If MQTT Retain enabled broker should keep the retain enabled message for future subscribers.
 * This samples demonstrates how MQTT retain feature works.
 */
public class Main {

    private static final Log log = LogFactory.getLog(Main.class);

    /**
     * Java temporary directory location
     */
    private static final String JAVA_TMP_DIR = System.getProperty("java.io.tmpdir");

    /**
     * The MQTT broker URL
     */
    private static final String brokerURL = "tcp://localhost:1883";

    /**
     * topic name for subscriber and publisher
     */
    private static String topic;

    /**
     * retain state for published topic
     */
    private static boolean retained;

    /**
     * The main method which runs the sample.
     *
     * @param args Commandline arguments
     */
    public static void main(String[] args) throws InterruptedException, IOException {

        // buffer reader for read console inputs
        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(System.in));

        String subscriberClientId = "subscriber";
        String publisherClientId = "publisher";

        int maxWaitTimeUntilReceiveMessages = 1000;

        // default topic name
        topic = "simpleTopic";

        // default retain state
        retained = true;

        // default payload
        byte[] payload = "sample message payload".getBytes();

        log.info("Retain Topic Sample");

        getUserInputs(bufferReader);

        log.info("Start sample with Topic Name " + topic + " with retain " + retained + ".");

        try {
            // Creating mqtt subscriber client
            MqttClient mqttSubscriberClient = getNewMqttClient(subscriberClientId);

            // Creating mqtt publisher client
            MqttClient mqttPublisherClient = getNewMqttClient(publisherClientId);

            // Publishing to mqtt topic "simpleTopic"
            mqttPublisherClient.publish(topic, payload, QualityOfService.LEAST_ONCE.getValue(),
                                        retained);
            log.info("Publish topic message with retain enabled for topic name " + topic);

            // Subscribing to mqtt topic "simpleTopic"
            mqttSubscriberClient.subscribe(topic, QualityOfService.LEAST_ONCE.getValue());
            log.info("Subscribe for topic name " + topic);

            Thread.sleep(maxWaitTimeUntilReceiveMessages);
            mqttPublisherClient.disconnect();
            mqttSubscriberClient.disconnect();

            log.info("Clients Disconnected!");
        } catch (MqttException e) {
            log.error("Error running the sample", e);
        } finally {
            bufferReader.close();
        }

    }

    /**
     * Read user inputs and set to relevant parameters
     *
     * @param bufferReader buffer text from character input stream
     * @throws IOException
     */
    private static void getUserInputs(BufferedReader bufferReader) throws IOException {

        String lineSeparator = System.getProperty("line.separator");

        log.info("Enter topic name: ");
        String bufferReaderString = bufferReader.readLine();

        if (!bufferReaderString.isEmpty()) {
            topic = bufferReaderString;
        } else {
            log.info("Topic name not valid. Continuing with default topic name : " + topic);
        }
        log.info("Set retain flag [Y/N]: ");
        bufferReaderString = bufferReader.readLine();

        if (bufferReaderString.equalsIgnoreCase("Y")) {
            // set retain enable
            retained = true;
        } else if (bufferReaderString.equalsIgnoreCase("N")) {
            // set retain disable
            retained = false;
        } else {
            log.info("Retain state not valid. Continuing with default retain state: " + retained);
        }
        log.info(lineSeparator + "Enter Y to continue with " + topic + " topic name and" +
                 " retain state " + retained + "." + lineSeparator +
                 "Enter N to revise parameters [Y/N]: ");

        bufferReaderString = bufferReader.readLine();
        if (bufferReaderString.equalsIgnoreCase("N")) {
            getUserInputs(bufferReader);
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
