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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Scanner;

/**
 * Represents a chat console.
 */
public final class ChatWindow {

    /**
     * The new line character
     */
    private static final String NEW_LINE = "\n";

    /**
     * Message header and content separating string
     */
    private static final String SEPARATOR = "::";

    /**
     * Scanner to read user input
     */
    private static final Scanner scanner = new Scanner(System.in);

    /**
     * Output StreamWriter to write to the console
     */
    private static final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));

    /**
     * The delimiter to separate each keyword in a user input command
     */
    private static final String COMMAND_DELIMITER = " ";

    /**
     * The command to exit
     */
    private static final String EXIT_COMMAND = "exit";

    /**
     * The command keyword to join a group chat
     */
    private static final String JOIN_GROUP_COMMAND = "join";

    /**
     * The command keyword to leave a group chat
     */
    private static final String LEAVE_GROUP_COMMAND = "leave";

    /**
     * The command keyword to get help
     */
    private static final String HELP_COMMAND = "help";

    /**
     * The command line helper string
     */
    private static final String HELP_STRING = "Use <alias/group message> to chat to a desired group or a person" +
            NEW_LINE + "<join group_name> to join a group chat" + NEW_LINE + "<leave group_name> to leave a group chat"
            + NEW_LINE + "<exit> to exit" + NEW_LINE;

    /**
     * Print a given message to the chat window console
     *
     * @param message The message to print to the console
     */
    public static void outputToChatWindow(String message) {
        try {
            writer.write(">" + message + NEW_LINE);
            writer.flush();
        } catch (IOException ignore) {
            // Silently ignore since there is no other way than this method itself to print to the output
        }
    }

    public static String getInputFromChatWindow() {
        return scanner.nextLine();
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
        StringBuilder output = new StringBuilder();

        if (null == chatName) {
            output.append("Personal message ");
        } else {
            output.append("chat with ").append(chatName).append(NEW_LINE);
        }

        String decoder[] = message.split(SEPARATOR);

        if (decoder.length == 1) { // Info message
            output.append("Info : ").append(decoder[0]);
        } else if (decoder.length == 2) { // chat message
            output.append("from ").append(decoder[0]).append(NEW_LINE).append(decoder[1]);
        } else {
            output.append("Invalid message received from the server.");
        }

        output.append(NEW_LINE).append("Waiting for your input. Use <help> for more info").append(NEW_LINE);

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
        return sender + SEPARATOR + message;
    }

    /**
     * Request and read user input from console giving a message to specify the request.
     *
     * @param message The input request message
     * @return User input line
     */
    public static String getInput(String message) {
        ChatWindow.outputToChatWindow(message);
        return getInputFromChatWindow();
    }

    /**
     * Directly read user input from the console. Use when user has already been notified about what to input.
     *
     * @return User input line
     */
    public static String getInput() {
        return getInputFromChatWindow();
    }

    /**
     * Process a given user input and take actions accordingly.
     * - Set exit flag
     * - Send messages
     * - Join a group conversation
     * - Leave a group conversation
     *
     * @param input      The user input line
     * @param chatClient The mqtt client to use when
     * @return Running condition
     * @throws MqttException
     */
    public static boolean processInput(String input, ChatClient chatClient) throws MqttException {
        boolean running = true;

        if (EXIT_COMMAND.equalsIgnoreCase(input)) {
            running = false;
        } else if (HELP_COMMAND.equalsIgnoreCase(input)) {
            printHelper();
        } else {
            String[] inputArgs = input.split(COMMAND_DELIMITER, 2);
            int argsLength = inputArgs.length;
            if (2 == argsLength) {
                String arg1 = inputArgs[0];
                String arg2 = inputArgs[1];
                if (JOIN_GROUP_COMMAND.equalsIgnoreCase(arg1)) {
                    chatClient.startGroupConversation(arg2);
                } else if (LEAVE_GROUP_COMMAND.equalsIgnoreCase(arg1)) {
                    chatClient.endGroupConversation(arg2);
                } else {
                    chatClient.sendMessage(arg1, arg2);
                }
            } else {
                outputToChatWindow("Incorrect command.");
                printHelper();
            }
        }

        return running;
    }

    /**
     * Print the help string to the output window.
     */
    public static void printHelper() {
        outputToChatWindow(HELP_STRING);
    }
}
