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

package org.wso2.mb.integration.common.clients.operations.mqtt.client.async;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.*;
import org.wso2.mb.integration.common.clients.operations.mqtt.QualityOfService;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.blocking.AndesMQTTBlockingClient;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.AndesMQTTClient;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.MQTTClientConnectionConfiguration;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.callback.CallbackHandler;

public abstract class AndesMQTTAsyncClient extends AndesMQTTClient {

    private final static Log log = LogFactory.getLog(AndesMQTTBlockingClient.class);

    protected MqttAsyncClient mqttClient;

    /**
     * Create a mqtt client initializing mqtt options.
     *
     * @param configuration   MQTT configurations
     * @param clientID        The unique client Id
     * @param topic           Topic to subscribe/publish to
     * @param qos             The quality of service
     * @param callbackHandler Callback Handler to handle receiving messages/message sending ack
     * @throws MqttException
     */
    public AndesMQTTAsyncClient(MQTTClientConnectionConfiguration configuration, String clientID, String topic,
                                   QualityOfService qos, CallbackHandler callbackHandler) throws MqttException {
        super(configuration, clientID, topic, qos, callbackHandler);

        // Construct MQTT client
        mqttClient = new MqttAsyncClient(this.broker_url, clientID, dataStore);

        // Connect to the MQTT server
        log.info("Connecting to " + broker_url + " with client ID " + mqttClientID);
        IMqttToken connectionToken = mqttClient.connect(connection_options);

        // Wait until connection is complete. Otherwise test results will be unpredictable
        connectionToken.waitForCompletion();

        log.info("Client " + mqttClientID + " Connected");

        mqttClient.setCallback(callbackHandler);
    }

    /**
     * Publish to mqtt.
     *
     * @param payload      Data to send
     * @param noOfMessages Number of message to send
     * @throws MqttException
     */
    protected void publish(byte[] payload, int noOfMessages) throws MqttException {
        log.info("Publishing to topic : " + topic + " on qos : " + qos);

        if (payload != null) {

            // Create and configure message
            MqttMessage message = new MqttMessage(payload);
            message.setQos(qos.getValue());

            for (int i = 0; i < noOfMessages; i++) {
                // Send message to server, control is either returned or blocked until it has been delivered to the
                // server depending on the MqttClient type (Blocking/Async)meeting the specified quality of service.
                mqttClient.publish(topic, message);
            }
        }
    }

    /**
     * Subscribe to a topic
     *
     * @throws MqttException
     */
    public void subscribe() throws MqttException {
        // Subscribe to the requested topic
        // The QoS specified is the maximum level that messages will be sent to the client at.
        // For instance if QoS 1 is specified, any messages originally published at QoS 2 will
        // be downgraded to 1 when delivering to the client but messages published at 1 and 0
        // will be received at the same level they were published at.
        log.info("Subscribing to topic \"" + topic + "\" qos " + qos);
        IMqttToken subscriptionToken = mqttClient.subscribe(topic, qos.getValue());

        // Wait until subscription is made. Otherwise test results will be unpredictable
        subscriptionToken.waitForCompletion();

        //Will need to wait to receive all messages - subscriber closes on shutdown
    }

    /**
     * Un-subscribe from the topic.
     *
     * @throws MqttException
     */
    public void unsubscribe() throws MqttException {
        IMqttToken unsubscriptionToken = mqttClient.unsubscribe(topic);

        // Wait until un-subscription is successful. Otherwise test results will be unpredictable.
        unsubscriptionToken.waitForCompletion();
        log.info("Subscriber for topic : " + topic + " un-subscribed");
    }

    /**
     * Shutdown the mqtt client. Call this whenever the system exits, test cases are finished or shutdown hook is
     * called.
     *
     * @throws MqttException
     */
    public void shutdown() throws MqttException {
        if (isConnected()) {
            IMqttToken disconnectionToken = mqttClient.disconnect();

            // Wait until shutdown is complete
            disconnectionToken.waitForCompletion();
            log.info("Client " + mqttClientID + " Disconnected");
        }
    }

    /**
     * Use this to validate if connection to server is still active.
     *
     * @return Is MQTT client connected to the server
     */
    public boolean isConnected() {
        return mqttClient.isConnected();
    }

}
