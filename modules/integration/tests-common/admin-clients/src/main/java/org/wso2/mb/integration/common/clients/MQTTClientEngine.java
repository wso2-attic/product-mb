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

package org.wso2.mb.integration.common.clients;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.apache.commons.lang.RandomStringUtils;
import org.wso2.mb.integration.common.clients.operations.mqtt.async.MQTTAsyncPublisherClient;
import org.wso2.mb.integration.common.clients.operations.mqtt.async.MQTTAsyncSubscriberClient;
import org.wso2.mb.integration.common.clients.operations.mqtt.blocking.MQTTBlockingPublisherClient;
import org.wso2.mb.integration.common.clients.operations.mqtt.blocking.MQTTBlockingSubscriberClient;

/**
 * Handle all MQTT operations for MQTT tests.
 * Create a new instance of this per each test case.
 */
public class MQTTClientEngine {

    /**
     * Keeps all the publishers created through the engine
     */
    private final List<AndesMQTTClient> publisherList = new ArrayList<AndesMQTTClient>();

    /**
     * Keep all the subscribers created through the engine
     */
    private final List<AndesMQTTClient> subscriberList = new ArrayList<AndesMQTTClient>();

    /**
     * Subscriber client thread executor, executes runnable subscribers
     */
    private final ExecutorService clientControlSubscriptionThreads = Executors.newFixedThreadPool(10);

    /**
     * Publisher client thread executor, executes runnable publishers
     */
    private final ExecutorService clientControlPublisherThreads = Executors.newFixedThreadPool(10);

    private final Log log = LogFactory.getLog(MQTTClientEngine.class);

    private static final int MILLISECONDS_TO_A_SECOND = 1000;

    /**
     * The executor service to invoke scheduled jobs
     */
    private final ScheduledExecutorService scheduleExecutor = Executors.newScheduledThreadPool(1);

    /**
     * Schedule which publishes send/receive TPS
     */
    private ScheduledFuture tpsPublisherSchedule;

    /**
     * The received message count there was when the previous TPS calculation happened
     */
    private int previousReceivedMessageCount;

    /**
     * The sent message count there was when the previous TPS calculation happened
     */
    private int previousSentMessageCount;

    /**
     * Initialises the client engine attaching a shutdown hook to close all the opened connection.
     * Initialises TPS publishing mechanism.
     */
    public MQTTClientEngine() {
        startTPSPublisher();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    shutdown();
                    log.info("All mqtt clients have been shutdown.");
                } catch (MqttException e) {
                    log.error("Error occurred invoking shutdown hook for " + this.getName(), e);
                }

            }
        });
    }

    /**
     * Generate a unique client Id for MQTT clients.
     * Randomize current timestamp.
     *
     * @return A unique Id
     */
    private String generateClientID() {
        String clientId = RandomStringUtils.random(MQTTConstants.CLIENT_ID_LENGTH, String.valueOf(System
                .currentTimeMillis()));
        log.info("ClientID generated : " + clientId);
        return clientId;
    }

    /**
     * Create a MQTT subscriber. Use when a subscriber with specific MQTTClientConnectionConfiguration is required.
     *
     * @param configuration MQTT configurations for the subscriber
     * @param topicName     Topic to subscribe to
     * @param qos           Quality of Service
     * @param saveMessages  Save receiving messages
     * @param clientMode    Client connection mode
     * @throws MqttException
     */
    public void createSubscriberConnection(MQTTClientConnectionConfiguration configuration,
                                           String topicName, QualityOfService qos,
                                           boolean saveMessages, ClientMode clientMode) throws MqttException {

        AndesMQTTClient mqttClient = null;

        if (ClientMode.ASYNC == clientMode) {
            mqttClient = new MQTTAsyncSubscriberClient(configuration, generateClientID(), topicName, qos, saveMessages);
        } else if (ClientMode.BLOCKING == clientMode) {
            mqttClient = new MQTTBlockingSubscriberClient(configuration, generateClientID(), topicName, qos,
                    saveMessages);
        } else {
            // Using else since only the above two scenarios are handled. If a new client mode is included,
            // handle it before this
            throw new MqttException(new Throwable("Unidentified clientMode : " + clientMode));
        }

        subscriberList.add(mqttClient);
        clientControlSubscriptionThreads.execute(mqttClient);

        waitForSubscribersToSubscribe();
    }

    /**
     * Wait until all the subscriber are subscribed to the topics and ready to receive messages.
     * Before creating publishers, this should be called otherwise while subscribers are being subscribed publishers
     * will start to publish and those messages will be lost.
     */
    private void waitForSubscribersToSubscribe() {
        while (!isAllSubscribersSubscribed()) {
            try {
                TimeUnit.SECONDS.sleep(1L);
            } catch (InterruptedException e) {
                log.error("Error waiting until subscribers subscribe to topics.", e);
            }
            log.info("Waiting for subscribers to create connection");
        }
    }

    /**
     * Check if all the subscribers are subscribed to the topics and ready to receive messages.
     *
     * @return Is all subscribers subscribed to their topics
     */
    private boolean isAllSubscribersSubscribed() {
        boolean subscribed = true;
        for (AndesMQTTClient subscriberClient : subscriberList) {
            if (!subscriberClient.isSubscribed()) {
                subscribed = false;
                break;
            }
        }

        return subscribed;
    }

    /**
     * Create a MQTT publisher. Use when a publisher with specific MQTTClientConnectionConfiguration is required.
     *
     * @param configuration MQTT MQTT configurations for the publisher
     * @param topicName     Topic to publish to
     * @param qos           Quality of Service
     * @param payload       Payload of the sending message
     * @param noOfMessages  Number of message to send
     * @param clientMode    Client connection mode
     * @throws MqttException
     */
    public void createPublisherConnection(MQTTClientConnectionConfiguration configuration,
                                          String topicName, QualityOfService qos, byte[] payload,
                                          int noOfMessages, ClientMode clientMode) throws MqttException {

        AndesMQTTClient mqttClient = null;

        if (ClientMode.ASYNC == clientMode) {
            mqttClient = new MQTTAsyncPublisherClient(configuration, generateClientID(), topicName, qos, payload,
                    noOfMessages);
        } else if (ClientMode.BLOCKING == clientMode) {
            mqttClient = new MQTTBlockingPublisherClient(configuration, generateClientID(), topicName, qos, payload,
                    noOfMessages);
        } else {
            // Using else since only the above two scenarios are handled. If a new client mode is included,
            // handle it before this
            throw new MqttException(new Throwable("Unidentified ClientMode : " + clientMode));
        }

        publisherList.add(mqttClient);
        clientControlPublisherThreads.execute(mqttClient);
    }

    /**
     * Create a given number of subscribers.
     *
     * @param topicName       Topic to subscribe to
     * @param qos             Quality of Service
     * @param noOfSubscribers Number of subscriber connections to create
     * @param saveMessages    Save receiving messages
     * @param clientMode      Client connection mode
     * @throws MqttException
     */
    public void createSubscriberConnection(String topicName, QualityOfService qos, int noOfSubscribers,
                                           boolean saveMessages, ClientMode clientMode) throws MqttException {
        MQTTClientConnectionConfiguration defaultConfigurations = getDefaultConfigurations();
        for (int i = 0; i < noOfSubscribers; i++) {
            createSubscriberConnection(defaultConfigurations, topicName, qos, saveMessages, clientMode);
        }
    }

    /**
     * Create a given number of publishers.
     *
     * @param topicName      Topic to publish to
     * @param qos            Quality of Service
     * @param payload        Payload of the sending message
     * @param noOfPublishers Number of publisher connections to create
     * @param noOfMessages   Number of message to send
     * @param clientMode     Client connection mode
     * @throws MqttException
     */
    public void createPublisherConnection(String topicName, QualityOfService qos, byte[] payload,
                                          int noOfPublishers, int noOfMessages, ClientMode clientMode) throws
            MqttException {
        MQTTClientConnectionConfiguration defaultConfigurations = getDefaultConfigurations();
        for (int i = 0; i < noOfPublishers; i++) {
            createPublisherConnection(defaultConfigurations, topicName, qos, payload, noOfMessages, clientMode);
        }
    }

    /**
     * Retrieve default MQTT client configurations. Always retrieve configurations from this unless there is a
     * specific requirement.
     *
     * @return Default MQTTClientConnectionConfigurations
     */
    private MQTTClientConnectionConfiguration getDefaultConfigurations() {
        MQTTClientConnectionConfiguration configuration = new MQTTClientConnectionConfiguration();

        configuration.setBrokerHost(MQTTConstants.BROKER_HOST);
        configuration.setBrokerProtocol(MQTTConstants.BROKER_PROTOCOL);
        configuration.setBrokerPort(MQTTConstants.BROKER_PORT);
        configuration.setBrokerPassword(MQTTConstants.BROKER_PASSWORD);
        configuration.setBrokerUserName(MQTTConstants.BROKER_USER_NAME);
        configuration.setCleanSession(true);

        return configuration;
    }

    /**
     * Get received messages from all subscriber clients.
     *
     * @return Received messages
     */
    public List<MqttMessage> getReceivedMessages() {
        List<MqttMessage> receivedMessages = new ArrayList<MqttMessage>();
        for (AndesMQTTClient subscriber : subscriberList) {
            receivedMessages.addAll(subscriber.getReceivedMessages());
        }

        return receivedMessages;
    }

    /**
     * Get received message count from all subscribers.
     *
     * @return Received message count
     */
    public int getReceivedMessageCount() {
        int count = 0;

        for (AndesMQTTClient subscriber : subscriberList) {
            count = count + subscriber.getReceivedMessageCount();
        }

        return count;
    }

    /**
     * Get sent message count from all publishers.
     *
     * @return Sent message count
     */
    public int getSentMessageCount() {
        int count = 0;

        for (AndesMQTTClient publisher : publisherList) {
            count = count + publisher.getSentMessageCount();
        }

        return count;
    }

    /**
     * Get all the subscribers.
     * Use if needed to directly handle subscribers.
     *
     * @return MQTTSubscriberClient list
     */
    public List<AndesMQTTClient> getSubscriberList() {
        return subscriberList;
    }

    /**
     * Get all the publishers.
     * Use if needed to directly handle publishers.
     *
     * @return MQTTPublisherClient list
     */
    public List<AndesMQTTClient> getPublisherList() {
        return publisherList;
    }

    /**
     * Wait for subscribers to receive all the messages and shutdown all clients.
     * Use in test cases before doing assertions so message send/receive will be completed before assertions.
     *
     * @see MQTTClientEngine#waitUntilAllMessageReceived()
     *
     * @throws MqttException
     */
    public void waitUntilAllMessageReceivedAndShutdownClients() throws MqttException {
        waitUntilAllMessageReceived();

        shutdown();
    }

    /**
     * Wait for subscribers to receive all the messages that have been sent.
     * Use in test cases before doing assertions so message send/receive will be completed before assertions
     * but needs the clients to be connected for further cases.
     * <p/>
     * Detect all the messages are received by checking message count in each 10 second iterations.
     * If message count doesn't change in two consecutive rounds it will be decided that all the messages that the
     * server has sent is received.
     * <p/>
     * If no messages are received, will lookout for 20 seconds for message and return.
     */
    public void waitUntilAllMessageReceived() {
        int previousMessageCount = 0;
        int currentMessageCount = -1;

        // Check each 10 second if new messages have been received, if not shutdown clients.
        // If no message are received this will wait for 20 seconds before shutting down clients.
        while (currentMessageCount != previousMessageCount) {
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                log.error("Error waiting for receiving messages.", e);
            }
            previousMessageCount = currentMessageCount;
            currentMessageCount = getReceivedMessageCount();
        }
    }

    /**
     * Calculate the TPS for the last (messageCount) messages.
     *
     * @param timeDiffMillis Time took in milliseconds to receive (messageCount) messages.
     * @return Transactions Per Second
     */
    private double calculateTPS(long timeDiffMillis, int messageCount) {
        return ((double) messageCount) / ((double) timeDiffMillis / MILLISECONDS_TO_A_SECOND);
    }

    /**
     * Start publishing message send/receive TPS.
     */
    private void startTPSPublisher() {
        // scheduling each second will be too much details, but greater than 10 will be too less details, hence 5
        final int scheduleTimeInSeconds = 5;
        tpsPublisherSchedule = scheduleExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                int currentReceivedMessageCount = getReceivedMessageCount();
                int currentSentMessageCount = getSentMessageCount();

                if (currentReceivedMessageCount != previousReceivedMessageCount) {
                    double receiveTPS = calculateTPS(scheduleTimeInSeconds * MILLISECONDS_TO_A_SECOND,
                            currentReceivedMessageCount - previousReceivedMessageCount);
                    log.info("Message Receiving TPS for the last " + scheduleTimeInSeconds + " seconds : " +
                            receiveTPS);

                    previousReceivedMessageCount = currentReceivedMessageCount;
                }

                if (currentSentMessageCount != previousSentMessageCount) {
                    double sentTPS = calculateTPS(scheduleTimeInSeconds * MILLISECONDS_TO_A_SECOND,
                            currentSentMessageCount - previousSentMessageCount);
                    log.info("Message Sending TPS for the last " + scheduleTimeInSeconds + " seconds : " + sentTPS);

                    previousSentMessageCount = currentSentMessageCount;
                }
            }
        }, 0, scheduleTimeInSeconds, TimeUnit.SECONDS);
    }

    /**
     * Shutdown all the clients.
     * Invoke when message send/receive is complete or shutdown hook is triggered.
     *
     * @throws MqttException
     */
    public void shutdown() throws MqttException {

        for (AndesMQTTClient subscriberClient : subscriberList) {
            subscriberClient.shutdown();
        }

        for (AndesMQTTClient publisherClient : publisherList) {
            publisherClient.shutdown();
        }

        tpsPublisherSchedule.cancel(true);
        scheduleExecutor.shutdown();
    }

}
