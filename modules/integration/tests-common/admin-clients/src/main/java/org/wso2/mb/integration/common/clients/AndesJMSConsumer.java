/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.mb.integration.common.clients;

import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSDeliveryStatus;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The JMS message consumer used for creating a consumer, reading messages synchronously and also
 * asynchronously.
 */
public class AndesJMSConsumer extends AndesJMSClient
        implements Runnable, MessageListener {
    /**
     * The logger used in logging information, warnings, errors and etc.
     */
    private static Logger log = Logger.getLogger(AndesJMSConsumer.class);

    /**
     * The configuration for the consumer
     */
    private final AndesJMSConsumerClientConfiguration consumerConfig;

    /**
     * Timestamp for the first message consumed
     */
    private AtomicLong firstMessageConsumedTimestamp;

    /**
     * Timestamp of the last message consumes
     */
    private AtomicLong lastMessageConsumedTimestamp;

    /**
     * The amount of messages received by the the consumer
     */
    private AtomicLong receivedMessageCount;

    /**
     * The addition of the time differences between the timestamp at which it got published and the
     * timestamp at which it got consumed for each message consumed.
     */
    private AtomicLong totalLatency;

    /**
     * The JMS connection used to create the JMS sessions
     */
    private Connection connection;

    /**
     * The JMS session used to create the JMS receiver
     */
    private Session session;

    /**
     * The receiver used the consume the received messages
     */
    private MessageConsumer receiver;

    /**
     * Creates a new JMS consumer with a given configuration.
     *
     * @param config The configuration.
     * @throws NamingException
     * @throws JMSException
     */
    public AndesJMSConsumer(AndesJMSConsumerClientConfiguration config)
            throws NamingException, JMSException {
        super(config);
        // Initializes variables used statistics calculations
        firstMessageConsumedTimestamp = new AtomicLong();
        lastMessageConsumedTimestamp = new AtomicLong();
        totalLatency = new AtomicLong();

        // Initializing message count variable.
        receivedMessageCount = new AtomicLong();

        // Sets the configuration
        this.consumerConfig = config;

        if (this.consumerConfig.getExchangeType() == ExchangeType.QUEUE) {
            this.createQueueConnection();

        } else if (this.consumerConfig.getExchangeType() == ExchangeType.TOPIC) {
            this.createTopicConnection();
        }
    }

    private void createTopicConnection() throws NamingException, JMSException {
        // Creates a topic connection, sessions and receiver
        TopicConnectionFactory connFactory = (TopicConnectionFactory) super.getInitialContext().lookup(AndesClientConstants.CF_NAME);
        TopicConnection topicConnection = connFactory.createTopicConnection();
        topicConnection.setClientID(this.consumerConfig.getSubscriptionID());
        topicConnection.start();
        TopicSession topicSession;
        // Sets acknowledgement mode
        if (this.consumerConfig.getAcknowledgeMode().getType() == TopicSession.SESSION_TRANSACTED) {
            topicSession = topicConnection.createTopicSession(true, this.consumerConfig.getAcknowledgeMode().getType());
        } else {
            topicSession = topicConnection.createTopicSession(false, this.consumerConfig.getAcknowledgeMode().getType());
        }

        Topic topic = (Topic) super.getInitialContext().lookup(this.consumerConfig.getDestinationName());

        connection = topicConnection;
        session = topicSession;
        // If topic is durable
        if (this.consumerConfig.isDurable()) {
            // If selectors exists
            if (null != this.consumerConfig.getSelectors()) {
                receiver = topicSession.createDurableSubscriber(topic, this.consumerConfig.getSubscriptionID(), this.consumerConfig.getSelectors(), false);
            } else {
                receiver = topicSession.createDurableSubscriber(topic, this.consumerConfig.getSubscriptionID());
            }
        } else {
            // If selectors exists
            if (null != this.consumerConfig.getSelectors()) {
                receiver = topicSession.createSubscriber(topic, this.consumerConfig.getSelectors(), false);
            } else {
                receiver = topicSession.createSubscriber(topic);
            }
        }
    }

    private void createQueueConnection() throws NamingException, JMSException {
        // Creates a queue connection, sessions and receiver
        QueueConnectionFactory connFactory = (QueueConnectionFactory) super.getInitialContext().lookup(AndesClientConstants.CF_NAME);
        QueueConnection queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        QueueSession queueSession;

        // Sets acknowledgement mode
        if (this.consumerConfig.getAcknowledgeMode().getType() == QueueSession.SESSION_TRANSACTED) {
            queueSession = queueConnection.createQueueSession(true, this.consumerConfig.getAcknowledgeMode().getType());
        } else {
            queueSession = queueConnection.createQueueSession(false, this.consumerConfig.getAcknowledgeMode().getType());
        }

        Queue queue = (Queue) super.getInitialContext().lookup(this.consumerConfig.getDestinationName());
        connection = queueConnection;
        session = queueSession;

        // If selectors exists
        if (null != this.consumerConfig.getSelectors()) {
            receiver = queueSession.createReceiver(queue, this.consumerConfig.getSelectors());
        } else {
            receiver = queueSession.createReceiver(queue);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startClient() throws JMSException, NamingException {
        Thread consumerThread = new Thread(this);
        consumerThread.start();
        log.info("Starting Consumer | ThreadID : " + consumerThread.getId());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stopClient() throws JMSException {
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
                try {
                    log.info("Closing Consumer");
                    if (ExchangeType.TOPIC == consumerConfig.getExchangeType()) {
                        if (null != receiver) {
                            TopicSubscriber topicSubscriber = (TopicSubscriber) receiver;
                            topicSubscriber.close();
                        }

                        if (null != session) {
                            TopicSession topicSession = (TopicSession) session;
                            topicSession.close();
                        }

                        if (null != connection) {
                            TopicConnection topicConnection = (TopicConnection) connection;
                            //topicConnection.stop();
                            topicConnection.close();
                        }
                    } else if (ExchangeType.QUEUE == consumerConfig.getExchangeType()) {
                        if (null != receiver) {
                            QueueReceiver queueReceiver = (QueueReceiver) receiver;
                            queueReceiver.close();
                        }

                        if (null != session) {
                            QueueSession queueSession = (QueueSession) session;
                            queueSession.close();
                        }

                        if (null != connection) {
                            QueueConnection queueConnection = (QueueConnection) connection;
                            queueConnection.close();
                        }
                    }

                    log.info("Consumer Closed");

                } catch (JMSException e) {
                    log.error("Error in stopping client.", e);
                    throw new RuntimeException("JMSException : Error in stopping client.", e);
                }
//            }
//        }).start();
    }

    /**
     * Un-Subscribes and closes a consumers.
     *
     * @throws JMSException
     */
    public void unSubscribe() throws JMSException {
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
                try {
                    log.info("Un-subscribing Subscriber");
                    session.unsubscribe(consumerConfig.getSubscriptionID());
                    log.info("Subscriber Un-Subscribed");
                    stopClient();

                } catch (JMSException e) {
                    log.error("Error in removing subscription(un-subscribing).", e);
                    throw new RuntimeException("JMSException : Error in removing subscription(un-subscribing).", e);
                }
//            }
//        }).start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try {
            if (this.consumerConfig.isAsync()) {
                // Use an asynchronous message listener
                receiver.setMessageListener(this);
            } else {
                // Uses a thread to listen to messages
                long threadID = Thread.currentThread().getId();
                while (true) {
                    Message message = this.receiver.receive();
                    if (null != message) {
                        // Calculating total latency
                        long currentTimeStamp = System.currentTimeMillis();
                        this.totalLatency.set(this.totalLatency.get() + (currentTimeStamp - message.getJMSTimestamp()));
                        // Setting timestamps for TPS calculation
                        if (this.firstMessageConsumedTimestamp.get() == 0) {
                            this.firstMessageConsumedTimestamp.set(currentTimeStamp);
                        }
                        this.lastMessageConsumedTimestamp.set(currentTimeStamp);

                        if (message instanceof TextMessage) {
                            this.receivedMessageCount.incrementAndGet();
                            String deliveryStatus;
                            TextMessage textMessage = (TextMessage) message;
                            // Gets whether the message is original or redelivered
                            if (message.getJMSRedelivered()) {
                                deliveryStatus = "REDELIVERED";
                            } else {
                                deliveryStatus = "ORIGINAL";
                            }
                            // Logging the received message
                            if (0 == this.receivedMessageCount.get() % this.consumerConfig.getPrintsPerMessageCount()) {
                                log.info("[RECEIVE] ThreadID:" + threadID + " Destination:" + this.consumerConfig.getExchangeType().getType()
                                         + "." + this.consumerConfig.getDestinationName() + " ReceivedMessageCount:" +
                                         this.receivedMessageCount.get() + " MaximumMessageToReceive:" +
                                         this.consumerConfig.getMaximumMessagesToReceived() + " Original/Redelivered:" + deliveryStatus);

                            }
                            // Writes the received messages
                            if (null != this.consumerConfig.getFilePathToWriteReceivedMessages()) {
                                AndesClientUtils.writeReceivedMessagesToFile(textMessage.getText(), this.consumerConfig.getFilePathToWriteReceivedMessages());
                            }
                            // Writes the statistics
                            if (null != this.consumerConfig.getFilePathToWriteStatistics()) {
                                String statisticsString = Long.toString(currentTimeStamp) + "," + Double.toString(this.getConsumerTPS()) + "," + Double.toString(this.getAverageLatency());
                                AndesClientUtils.writeStatisticsToFile(statisticsString, this.consumerConfig.getFilePathToWriteStatistics());
                            }
                        }

                        // Acknowledges messages
                        if (0 == this.receivedMessageCount.get() % this.consumerConfig.getAcknowledgeAfterEachMessageCount()) {
                            if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                                message.acknowledge();
                                log.info("Acknowledging message : " + message.getJMSMessageID());
                            }
                        }

                        if (0 == this.receivedMessageCount.get() % consumerConfig.getCommitAfterEachMessageCount()) {
                            // Committing session
                            session.commit();
                            log.info("Committed session");
                        } else if (0 == this.receivedMessageCount.get() % consumerConfig.getRollbackAfterEachMessageCount()) {
                            // Roll-backing session
                            session.rollback();
                            log.info("Roll-backed session");
                        }

                        if (this.receivedMessageCount.get() >= consumerConfig.getUnSubscribeAfterEachMessageCount()) {
                            // Un-Subscribing consumer
                            unSubscribe();
                            break;
                        } else if (this.receivedMessageCount.get() >= consumerConfig.getMaximumMessagesToReceived()) {
                            // Stopping the consumer
                            stopClient();
                            break;
                        }

                        // Delaying reading of messages
                        if (0 < consumerConfig.getRunningDelay()) {
                            try {
                                Thread.sleep(consumerConfig.getRunningDelay());
                            } catch (InterruptedException e) {
                                //silently ignore
                            }
                        }
                    }
                }

            }
        } catch (JMSException e) {
            log.error("Error while listening to messages", e);
            throw new RuntimeException("JMSException : Error while listening to messages", e);
        } catch (IOException e) {
            log.error("Error while writing message to file", e);
            throw new RuntimeException("IOException : Error while writing message to file\"", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onMessage(Message message) {
        try {
            // Calculating total latency
            long currentTimeStamp = System.currentTimeMillis();
            this.totalLatency.set(this.totalLatency.get() + (currentTimeStamp - message.getJMSTimestamp()));
            // Setting timestamps for TPS calculation
            if (0 == this.firstMessageConsumedTimestamp.get()) {
                this.firstMessageConsumedTimestamp.set(currentTimeStamp);
            }

            this.lastMessageConsumedTimestamp.set(currentTimeStamp);
            if (message instanceof TextMessage) {
                this.receivedMessageCount.incrementAndGet();
                long threadID = Thread.currentThread().getId();
                JMSDeliveryStatus deliveryStatus;
                TextMessage textMessage = (TextMessage) message;
                // Gets whether the message is original or redelivered
                if (message.getJMSRedelivered()) {
                    deliveryStatus = JMSDeliveryStatus.REDELIVERED;
                } else {
                    deliveryStatus = JMSDeliveryStatus.ORIGINAL;
                }
                // Logging the received message
                if (0 == this.receivedMessageCount.get() % this.consumerConfig.getPrintsPerMessageCount()) {
                    log.info("[RECEIVE] ThreadID:" + threadID + " Destination:" + this.consumerConfig.getExchangeType().getType()
                             + "." + this.consumerConfig.getDestinationName() + " ReceivedMessageCount:" +
                             this.receivedMessageCount.get() + " MaximumMessageToReceive:" +
                             this.consumerConfig.getMaximumMessagesToReceived() + " Original/Redelivered:" + deliveryStatus.getStatus());

                }
                // Writes the received messages
                if (null != this.consumerConfig.getFilePathToWriteReceivedMessages()) {
                    AndesClientUtils.writeReceivedMessagesToFile(textMessage.getText(), this.consumerConfig.getFilePathToWriteReceivedMessages());
                }
                // Writes the statistics
                if (null != this.consumerConfig.getFilePathToWriteStatistics()) {
                    String statisticsString = Long.toString(currentTimeStamp) + "," + Double.toString(this.getConsumerTPS()) + "," + Double.toString(this.getAverageLatency());
                    AndesClientUtils.writeStatisticsToFile(statisticsString, this.consumerConfig.getFilePathToWriteStatistics());
                }
            }

            // Acknowledges messages
            if (0 == this.receivedMessageCount.get() % this.consumerConfig.getAcknowledgeAfterEachMessageCount()) {
                if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                    message.acknowledge();
                    log.info("Acknowledging message(JMSMessageID) : " + message.getJMSMessageID());
                }
            }

            if (0 == this.receivedMessageCount.get() % consumerConfig.getCommitAfterEachMessageCount()) {
                // Committing session
                session.commit();
                log.info("Committed session");
            } else if (0 == this.receivedMessageCount.get() % consumerConfig.getRollbackAfterEachMessageCount()) {
                // Roll-backing session
                session.rollback();
                log.info("Roll-backed session");
            }

            if (receivedMessageCount.get() >= consumerConfig.getUnSubscribeAfterEachMessageCount()) {
                // Un-Subscribing consumer
                unSubscribe();
                // TODO : revisit sleep
//                AndesClientUtils.sleepForInterval(500L);
            } else if (this.receivedMessageCount.get() >= consumerConfig.getMaximumMessagesToReceived()) {
                // Stopping the consumer
                // TODO : revisit sleep
                AndesClientUtils.sleepForInterval(1000L);
                stopClient();
//                AndesClientUtils.sleepForInterval(500L);
            }

            // Delaying reading of messages
            if (0 < consumerConfig.getRunningDelay()) {
                try {
                    Thread.sleep(consumerConfig.getRunningDelay());
                } catch (InterruptedException e) {
                    //silently ignore
                }
            }
        } catch (JMSException e) {
            log.error("Error while listening to messages", e);
            throw new RuntimeException("JMSException : Error while listening to messages", e);
        } catch (IOException e) {
            log.error("Error while writing message to file", e);
            throw new RuntimeException("IOException : Error while writing message to file", e);
        }
    }

    /**
     * Gets the received message count for the consumer.
     *
     * @return The received message count.
     */
    public long getReceivedMessageCount() {
        return this.receivedMessageCount.get();
    }

    /**
     * Gets the consumer transactions per seconds.
     *
     * @return The consumer transactions per seconds.
     */
    public double getConsumerTPS() {
        if (0 == this.lastMessageConsumedTimestamp.get() - this.firstMessageConsumedTimestamp.get()) {
            return this.receivedMessageCount.doubleValue() / (1D / 1000);
        } else {
            return this.receivedMessageCount.doubleValue() / ((this.lastMessageConsumedTimestamp.doubleValue() - this.firstMessageConsumedTimestamp.doubleValue()) / 1000D);
        }
    }

    /**
     * Gets the average latency for the consumer in receiving messages.
     *
     * @return The average latency.
     */
    public double getAverageLatency() {
        if (0 == this.receivedMessageCount.get()) {
            log.warn("No messages were received");
            return 0D;
        } else {
            return (this.totalLatency.doubleValue() / 1000D) / this.receivedMessageCount.doubleValue();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesJMSConsumerClientConfiguration getConfig() {
        return this.consumerConfig;
    }
}
