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

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.io.File;

/**
 * The MQTT clients which is used by the chat client to send/receive messages.
 */
public class AndesMQTTClient implements MqttCallback {

    /**
     * The Message Broker URL
     */
    private static final String brokerURL = "tcp://localhost:1883";

    /**
     * The temporary directory for mqtt client to work with
     */
    private static final String tmpDir = System.getProperty("java.io.tmpdir");

    /**
     * The MQTT client which is used to communicate with the server
     */
    private MqttClient mqttClient;

    /**
     * The unique MQTT client Id
     */
    private final String clientId;

    
    /**
     * Credentials to be used when connecting to MQTT server
     */
    private static final String DEFAULT_USER_NAME = "admin";
    
    private static final String DEFAULT_PASSWORD = "admin";
    
    
    /**
     * Create a new MQTT client with the given client Id. Return after the connection is successful.
     *
     * @param clientId The unique client Id
     * @throws MqttException
     */
    public AndesMQTTClient(String clientId) throws MqttException {
        this.clientId = clientId;
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setUserName(DEFAULT_USER_NAME);
        options.setPassword(DEFAULT_PASSWORD.toCharArray());
        
        mqttClient = new MqttClient(brokerURL, clientId, new MqttDefaultFilePersistence(tmpDir + File.separator +
                clientId));
        mqttClient.setCallback(this);
        mqttClient.connect(options);
    }

    /**
     * Subscribe to a given topic in given qos. Return after the subscription is complete.
     *
     * @param topic The topic to subscribe to
     * @param qos   The quality of service
     * @throws MqttException
     */
    public void subscribe(String topic, int qos) throws MqttException {
        mqttClient.subscribe(topic, qos);
    }

    /**
     * Un-subscribe from a given topic after publishing to the chat server that this client is going to leave the
     * given chat.
     *
     * @param topic The topic to un-subscribe from
     * @throws MqttException
     */
    public void unsubscribe(String topic) throws MqttException {
        mqttClient.publish(topic, (clientId + " has left the conversation").getBytes(), 2, false);
        mqttClient.unsubscribe(topic);
    }

    /**
     * Send message to a given topic.
     *
     * @param topic   The topic to send message to
     * @param message The message to send
     * @param qos     The quality of service
     * @throws MqttException
     */
    public void sendMessage(String topic, String message, int qos) throws MqttException {
        String encodedMessage = ChatWindow.encodeMessage(clientId, message);
        mqttClient.publish(topic, encodedMessage.getBytes(), qos, false);
    }

    /**
     * Disconnect the mqtt client from the server.
     *
     * @throws MqttException
     */
    public void disconnect() throws MqttException {
        mqttClient.disconnect();
    }

    /**
     * Handle if connection is lost with the server.
     *
     * @param throwable Cause
     */
    @Override
    public void connectionLost(Throwable throwable) {
        ChatWindow.outputToChatWindow("Connection lost");
    }

    /**
     * Handle receiving a message from the server.
     *
     * @param topic       The topic message received from
     * @param mqttMessage The received message
     * @throws Exception
     */
    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        synchronized (this.getClass()) {
            String chatFrom = null;

            // If message is received through the personal channel it is a personal message. Otherwise it is a group
            // chat
            if (!clientId.equals(topic)) {
                chatFrom = topic;
            }

            ChatWindow.decodeAndOutputMessage(chatFrom, mqttMessage.toString());
        }
    }

    /**
     * On delivery complete notify it to the chat console.
     *
     * @param iMqttDeliveryToken Delivery information token
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        synchronized (this.getClass()) {
            for (String topic : iMqttDeliveryToken.getTopics()) {
                String chatName = null;
                if (!topic.equals(clientId)) {
                    chatName = topic;
                }
                ChatWindow.decodeAndOutputMessage(chatName, "Message Sent");
            }
        }
    }

}
