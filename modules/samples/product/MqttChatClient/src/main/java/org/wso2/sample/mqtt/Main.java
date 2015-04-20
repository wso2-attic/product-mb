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

import java.util.concurrent.TimeUnit;

/**
 * This sample demonstrates how to use WSO2 Message Broker to create a chat client which uses MQTT.
 * <p/>
 * The Main class which executes the sample.
 * - Creates several chat clients
 * - Initiates personal conversations
 * - Initiates group conversations
 */
public class Main {

    private static ChatClient chatClient;
    private static boolean running = true;

    /**
     * The main method which invokes the sample.
     * - Creates a chat client
     * - Takes user input
     *
     * @param args Command line arguments
     * @throws MqttException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws MqttException, InterruptedException {

        String alias = ChatWindow.getInput("Please enter your chat alias : ");

        chatClient = new ChatClient(alias);

        ChatWindow.printHelper();

        while (running) {
            String input = ChatWindow.getInput();
            running = ChatWindow.processInput(input, chatClient);

            TimeUnit.SECONDS.sleep(1L);
        }

        disconnect();
    }

    /**
     * Disconnect all the chat clients from the server.
     *
     * @throws MqttException
     */
    private static void disconnect() throws MqttException {
        chatClient.closeClient();
    }


}
