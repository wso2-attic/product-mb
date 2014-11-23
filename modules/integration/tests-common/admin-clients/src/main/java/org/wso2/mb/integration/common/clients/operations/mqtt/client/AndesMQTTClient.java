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

package org.wso2.mb.integration.common.clients.operations.mqtt.client;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.wso2.mb.integration.common.clients.operations.mqtt.QualityOfService;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.callback.CallbackHandler;

import java.util.List;

/**
 * Basic MQTT client which handles the operations around MQTT clients.
 * - Handling fields
 * - Handling message counts
 * <p/>
 * Each MQTT client with different publish/subscribe mechanism should extend from this.
 */
public abstract class AndesMQTTClient implements Runnable {

    // The MQTT callback handler which handles message arrival, delivery complete and connection loss requests.
    private final CallbackHandler callbackHandler;

    // unique identifier for mqtt client - less than or equal to 23 characters
    protected final String mqttClientID;

    // Connection options that are required to create a connection to a MQTT server
    protected final MqttConnectOptions connection_options;

    // Message broker MQTT URL
    protected final String broker_url;

    // The topic the messages needs to send to / received from
    protected final String topic;

    // The quality of service to send/receive messages
    protected final QualityOfService qos;

    //Store messages until server fetches them
    protected final MqttDefaultFilePersistence dataStore = new MqttDefaultFilePersistence(System.getProperty("java.io" +
            ".tmpdir"));

    /**
     * Create a mqtt client initializing mqtt options.
     *
     * @param configuration   MQTT configurations
     * @param clientID        The unique client Id
     * @param topic           Topic to subscribe/publish to
     * @param qos             The quality of service
     * @param callbackHandler Callback Handler to handle receiving messages/message sending ack
     */
    public AndesMQTTClient(MQTTClientConnectionConfiguration configuration, String clientID, String topic,
                           QualityOfService qos, CallbackHandler callbackHandler) {

        //Initializing the variables locally
        this.broker_url = configuration.getBrokerURL();
        this.mqttClientID = clientID;
        String password = configuration.getBrokerPassword();
        String userName = configuration.getBrokerUserName();
        this.topic = topic;
        this.qos = qos;

        // Construct the connection options object that contains connection parameters
        // such as cleanSession and LWT
        connection_options = new MqttConnectOptions();
        connection_options.setCleanSession(configuration.isCleanSession());

        if (null != password) {
            connection_options.setPassword(password.toCharArray());
        }
        if (null != userName) {
            connection_options.setUserName(userName);
        }

        // Set callback handler
        this.callbackHandler = callbackHandler;
    }

    /**
     * Publish to mqtt.
     *
     * @param payload      Data to send
     * @param noOfMessages Number of message to send
     * @throws MqttException
     */
    protected abstract void publish(byte[] payload, int noOfMessages) throws MqttException;

    /**
     * Subscribe to the requested topic
     * The {@link QualityOfService} specified is the maximum level that messages will be sent to the client at.
     * For instance if QoS {@link QualityOfService#LEAST_ONCE} is specified, any messages originally published at QoS
     * {@link QualityOfService#EXACTLY_ONCE} will be downgraded to {@link QualityOfService#MOST_ONCE} when delivering
     * to the client but messages published at {@link QualityOfService#LEAST_ONCE} and {@link
     * QualityOfService#MOST_ONCE} will be received at the same level they were published at.
     *
     * @throws MqttException
     */
    public abstract void subscribe() throws MqttException;

    /**
     * Un-subscribe from the topic.
     *
     * @throws MqttException
     */
    public abstract void unsubscribe() throws MqttException;

    /**
     * Get the received message count from the callback handler to validate message receiving is successful.
     *
     * @return Received message count
     */
    public int getReceivedMessageCount() {
        int messageCount = 0;
        if (null != callbackHandler) {
            messageCount = callbackHandler.getReceivedMessageCount();
        }

        return messageCount;
    }

    /**
     * Get the sent message count from the callback handler to validate message sending is successful.
     *
     * @return The sent message count.
     */
    public int getSentMessageCount() {
        int messageCount = 0;
        if (null != callbackHandler) {
            messageCount = callbackHandler.getSentMessageCount();
        }

        return messageCount;
    }

    /**
     * Shutdown the mqtt client. Call this whenever the system exits, test cases are finished or shutdown hook is
     * called.
     *
     * @throws MqttException
     */
    public abstract void shutdown() throws MqttException;

    /**
     * Get the mqtt client Id. Use this to print client Id into logs whenever necessary.
     *
     * @return MQTT client Id
     */
    public String getMqttClientID() {
        return mqttClientID;
    }

    /**
     * Use this to validate if connection to server is still active.
     *
     * @return Is MQTT client connected to the server
     */
    public abstract boolean isConnected();

    /**
     * Get the topic name this MQTT client is connected to.
     *
     * @return The topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Get the MQTT callback handler for the client.
     *
     * @return The callback handler
     */
    public CallbackHandler getCallbackHandler() {
        return callbackHandler;
    }

    /**
     * Check if the subscriber is subscribed to a topic
     *
     * @return Is Subscribed
     */
    public abstract boolean isSubscribed();

    /**
     * Get all the received messages through this client.
     * Use this if want to validate message content.
     *
     * @return Received messages.
     */
    public abstract List<MqttMessage> getReceivedMessages();

}
