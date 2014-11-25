/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.Console;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a chat console.
 */
public final class ChatWindow {

    private static final Log log = LogFactory.getLog(ChatWindow.class);

    private static String newLine = "\n";

    // Message header and content separating string
    private static String seperator = "::";

    private static final Console console = System.console();
    private static final PrintWriter writer = console.writer();

    private static final String commandDelimiter = " ";

    private static AtomicBoolean lastInputProcessed = new AtomicBoolean(false);

    private static Thread inputThread;

    /**
     * Print a given message to the chat window console
     *
     * @param message
     */
    public static void outputToChatWindow(String message) {
        writer.print(message);
        writer.flush();
    }

    public static String getInputFromChatWindow() {
        return console.readLine();
    }

    /**
     * Decode a given message and output to the chat window console.
     * This is invoked when a new message is received to the chat client.
     *
     * @param chatName The chat name to decide on which chat the message should be shown given that there are
     *                 multiple active chats
     * @param message  The received message
     */
    public static void decodeAndOutputMessage(String chatName, String message) {
        StringBuffer output = new StringBuffer();

        if (chatName == null) {
            output.append("Personal message ");
        } else {
            output.append("chat with ").append(chatName).append(newLine);
        }
        String decoder[] = message.split(seperator);

        if (decoder.length == 1) { // Info message
            output.append("Info : ").append(decoder[0]);
        } else if (decoder.length == 2) {
            output.append("from ").append(decoder[0]).append(newLine).append(decoder[1]);
        } else {
            output.append("server error...!!!");
        }

        output.append(newLine).append("Waiting for your input").append(newLine);

        outputToChatWindow(output.toString());
    }

    /**
     * Encode a given message with sender::message.
     *
     * @param sender  The message sender Id
     * @param message The message to send
     * @return The encoded message
     */
    public static String encodeMessage(String sender, String message) {
        return sender + seperator + message;
    }

    public static String getInput(String message) {
        ChatWindow.outputToChatWindow(message);
        return console.readLine();
    }

    private static void getInputFromConsole(final ChatClient chatClient) {
        lastInputProcessed.set(false);
        inputThread = new Thread(new Runnable() {
            @Override
            public void run() {
                String input = console.readLine();
                try {
                    processInput(input, chatClient);
                } catch (MqttException e) {
                    log.error("Error processing input.", e);
                }
            }
        });

        inputThread.start();
    }

    public static void processInput(String input, ChatClient chatClient) throws MqttException {
        String[] inputArgs = input.split(commandDelimiter, 2);
        int argsLength = inputArgs.length;
        if (2 == argsLength) {
            chatClient.sendMessage(inputArgs[0], inputArgs[1]);
        } else {

        }

        lastInputProcessed.set(true);
    }
}
