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

import org.eclipse.paho.client.mqttv3.MqttException;

/**
 * Represents a chat client which hosts a mqtt client.
 */
public class ChatClient {

    // For a chat client messages should be received exactly once, which is qos 2 in MQTT.
    private static final int qos = 2;

    private AndesMQTTClient mqttClient;

    /**
     * Create a chat client an initialises a mqtt client on it's name.
     *
     * @param name The name of the chat client
     * @throws MqttException
     */
    public ChatClient(String name) throws MqttException {
        mqttClient = new AndesMQTTClient(name);
        mqttClient.subscribe(name, qos);
    }

    /**
     * Start/Join a group chat.
     *
     * @param groupName The group name
     * @throws MqttException
     */
    public void startGroupConversation(String groupName) throws MqttException {
        mqttClient.subscribe(groupName, qos);
        ChatWindow.outputToChatWindow("Joined to the group : " + groupName);
    }

    /**
     * Leave a group chat.
     *
     * @param groupName The group name
     * @throws MqttException
     */
    public void endGroupConversation(String groupName) throws MqttException {
        mqttClient.unsubscribe(groupName);
        ChatWindow.outputToChatWindow("Left the group : " + groupName);
    }

    /**
     * Send a chat message.
     *
     * @param chatName The person/group to send message to
     * @param message  The message
     * @throws MqttException
     */
    public void sendMessage(String chatName, String message) throws MqttException {
        mqttClient.sendMessage(chatName, message, qos);
    }

    /**
     * Close the chat client.
     *
     * @throws MqttException
     */
    public void closeClient() throws MqttException {
        mqttClient.disconnect();
    }
}
