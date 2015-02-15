package org.wso2.mb.integration.common.clients;

import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

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
import java.util.concurrent.atomic.AtomicLong;

class AndesJMSConsumer extends AndesJMSClient
        implements Runnable, MessageListener {
    private static Logger log = Logger.getLogger(AndesJMSConsumer.class);

    private final AndesJMSConsumerClientConfiguration consumerConfig;
    private AtomicLong firstMessageConsumedTimestamp;
    private AtomicLong lastMessageConsumedTimestamp;
    private AtomicLong receivedMessageCount;
    private AtomicLong totalLatency;
    private Connection connection;
    private Session session;
    private MessageConsumer receiver;

    AndesJMSConsumer(AndesJMSConsumerClientConfiguration config)
            throws NamingException, JMSException {
        super(config);
        firstMessageConsumedTimestamp = new AtomicLong();
        lastMessageConsumedTimestamp = new AtomicLong();
        totalLatency = new AtomicLong();
        receivedMessageCount = new AtomicLong();

        this.consumerConfig = config;
        if (this.consumerConfig.getExchangeType() == ExchangeType.QUEUE) {
            QueueConnectionFactory connFactory = (QueueConnectionFactory) super.getInitialContext().lookup(AndesClientConstants.CF_NAME);
            QueueConnection queueConnection = connFactory.createQueueConnection();
            queueConnection.start();

            QueueSession queueSession;
            if (this.consumerConfig.getAcknowledgeMode().getType() == QueueSession.SESSION_TRANSACTED) {
                queueSession = queueConnection.createQueueSession(true, this.consumerConfig.getAcknowledgeMode().getType());
            } else {
                queueSession = queueConnection.createQueueSession(false, this.consumerConfig.getAcknowledgeMode().getType());
            }

            Queue queue = (Queue) super.getInitialContext().lookup(this.consumerConfig.getDestinationName());
            connection = queueConnection;
            session = queueSession;
            receiver = queueSession.createReceiver(queue);
        } else if (this.consumerConfig.getExchangeType() == ExchangeType.TOPIC) {
            TopicConnectionFactory connFactory = (TopicConnectionFactory) super.getInitialContext().lookup(AndesClientConstants.CF_NAME);
            TopicConnection topicConnection = connFactory.createTopicConnection();
            topicConnection.setClientID(this.consumerConfig.getSubscriptionID());
            topicConnection.start();
            TopicSession topicSession;
            if (this.consumerConfig.getAcknowledgeMode().getType() == TopicSession.SESSION_TRANSACTED) {
                topicSession = topicConnection.createTopicSession(true, this.consumerConfig.getAcknowledgeMode().getType());
            } else {
                topicSession = topicConnection.createTopicSession(false, this.consumerConfig.getAcknowledgeMode().getType());
            }

            // Send message
            Topic topic = (Topic) super.getInitialContext().lookup(this.consumerConfig.getDestinationName());

            connection = topicConnection;
            session = topicSession;
            if (this.consumerConfig.isDurable()) {
                receiver = topicSession.createDurableSubscriber(topic, this.consumerConfig.getSubscriptionID());
            } else {
                receiver = topicSession.createSubscriber(topic);
            }
        }
    }

    @Override
    public void startClient() throws JMSException, NamingException {
        Thread consumerThread = new Thread(this);
        consumerThread.start();
        log.info("Starting Consumer | ThreadID : " + consumerThread.getId());
    }

    @Override
    public void stopClient() throws JMSException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // TODO : remove duplication if possible
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
            }
        }).start();
    }

    public void unSubscribe() throws JMSException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("Un-subscribing Subscriber");
                    session.unsubscribe(consumerConfig.getSubscriptionID());
                    log.info("Subscriber Un-Subscribed");
                    stopClient();

                } catch (JMSException e) {
                    log.error("Error in removing subscription(un-subscribing).", e);
                    throw new RuntimeException("JMSException : Error in removing subscription(un-subscribing).", e);
                }
            }
        }).start();
    }

    @Override
    public void run() {
        try {
            if (this.consumerConfig.isAsync()) {
                receiver.setMessageListener(this);
            } else {
                long threadID = Thread.currentThread().getId();
                while (true) {
                    Message message = this.receiver.receive();
                    if (null != message) {
                        // calculating total latency
                        long currentTimeStamp = System.currentTimeMillis();
                        this.totalLatency.set(this.totalLatency.get() + (currentTimeStamp - message.getJMSTimestamp()));
                        // setting timestamps for TPS calculation
                        if (this.firstMessageConsumedTimestamp.get() == 0) {
                            this.firstMessageConsumedTimestamp.set(currentTimeStamp);
                        }

                        this.lastMessageConsumedTimestamp.set(currentTimeStamp);

                        if (message instanceof TextMessage) {
                            this.receivedMessageCount.incrementAndGet();
                            String redelivery;
                            TextMessage textMessage = (TextMessage) message;
                            if (message.getJMSRedelivered()) {
                                redelivery = "REDELIVERED";
                            } else {
                                redelivery = "ORIGINAL";
                            }
                            if (0 == this.receivedMessageCount.get() % this.consumerConfig.getPrintsPerMessageCount()) {
                                log.info("[RECEIVE] ThreadID:" + threadID + " Destination:" +
                                         this.consumerConfig.getDestinationName() + " TotalMessageCount:" +
                                         this.receivedMessageCount.get() + " MaximumMessageToReceive:" +
                                         this.consumerConfig.getMaximumMessagesToReceived() + " Original/Redelivered:" + redelivery);

                            }
                            if (null != this.consumerConfig.getFilePathToWriteReceivedMessages()) {
                                AndesClientUtils.writeReceivedMessagesToFile(textMessage.getText(), this.consumerConfig.getFilePathToWriteReceivedMessages());
                            }
                            if (null != this.consumerConfig.getFilePathToWriteStatistics()) {
                                String statisticsString = Long.toString(currentTimeStamp) + "," + Double.toString(this.getConsumerTPS()) + "," + Double.toString(this.getAverageLatency());
                                AndesClientUtils.writeStatisticsToFile(statisticsString, this.consumerConfig.getFilePathToWriteStatistics());
                            }
                        }

                        if (0 == this.receivedMessageCount.get() % this.consumerConfig.getAcknowledgeAfterEachMessageCount()) {
                            if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                                message.acknowledge();
                                log.info("Acknowledging message : " + message.getJMSMessageID());
                            }
                        }

                        //commit get priority
                        if (0 == this.receivedMessageCount.get() % consumerConfig.getCommitAfterEachMessageCount()) {
                            session.commit();
                            log.info("Committed session");
                        } else if (0 == this.receivedMessageCount.get() % consumerConfig.getRollbackAfterEachMessageCount()) {
                            session.rollback();
                            log.info("Roll-backed session");
                        }

                        if (this.receivedMessageCount.get() >= consumerConfig.getUnSubscribeAfterEachMessageCount()) {
                            unSubscribe();
                            break;
                        } else if (this.receivedMessageCount.get() >= consumerConfig.getMaximumMessagesToReceived()) {
                            stopClient();
                            break;
                        }

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
        }
    }

    @Override
    public void onMessage(Message message) {
        try {
            // calculating total latency
            long currentTimeStamp = System.currentTimeMillis();
            this.totalLatency.set(this.totalLatency.get() + (currentTimeStamp - message.getJMSTimestamp()));
            // setting timestamps for TPS calculation
            if (0 == this.firstMessageConsumedTimestamp.get()) {
                this.firstMessageConsumedTimestamp.set(currentTimeStamp);
            }

            this.lastMessageConsumedTimestamp.set(currentTimeStamp);

            if (message instanceof TextMessage) {
                this.receivedMessageCount.incrementAndGet();
                long threadID = Thread.currentThread().getId();
                String redelivery;
                TextMessage textMessage = (TextMessage) message;
                if (message.getJMSRedelivered()) {
                    redelivery = "REDELIVERED";
                } else {
                    redelivery = "ORIGINAL";
                }
                if (0 == this.receivedMessageCount.get() % this.consumerConfig.getPrintsPerMessageCount()) {
                    log.info("[DESTINATION RECEIVE] ThreadID:" + threadID + " Destination:" +
                             this.consumerConfig.getDestinationName() + " TotalMessageCount:" +
                             this.receivedMessageCount.get() + " MaximumMessageToReceive:" +
                             this.consumerConfig.getMaximumMessagesToReceived() + " Original/Redelivered :" + redelivery);

                }
                if (null != this.consumerConfig.getFilePathToWriteReceivedMessages()) {
                    AndesClientUtils.writeReceivedMessagesToFile(textMessage.getText(), this.consumerConfig.getFilePathToWriteReceivedMessages());
                }
                if (null != this.consumerConfig.getFilePathToWriteStatistics()) {
                    String statisticsString = Long.toString(currentTimeStamp) + "," + Double.toString(this.getConsumerTPS()) + "," + Double.toString(this.getAverageLatency());
                    AndesClientUtils.writeStatisticsToFile(statisticsString, this.consumerConfig.getFilePathToWriteStatistics());
                }
            }

            if (0 == this.receivedMessageCount.get() % this.consumerConfig.getAcknowledgeAfterEachMessageCount()) {
                if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                    message.acknowledge();
                    log.info("Acknowledging message : " + message.getJMSMessageID());
                }
            }

            //commit get priority
            if (0 == this.receivedMessageCount.get() % consumerConfig.getCommitAfterEachMessageCount()) {
                session.commit();
                log.info("Committed session");
            } else if (0 == this.receivedMessageCount.get() % consumerConfig.getRollbackAfterEachMessageCount()) {
                session.rollback();
                log.info("Roll-backed session");
            }

            if (receivedMessageCount.get() >= consumerConfig.getUnSubscribeAfterEachMessageCount()) {
                unSubscribe();
                AndesClientUtils.sleepForInterval(500L);
            } else if (this.receivedMessageCount.get() >= consumerConfig.getMaximumMessagesToReceived()) {
                stopClient();
                AndesClientUtils.sleepForInterval(500L);

            }

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
        }
    }

    public long getReceivedMessageCount() {
        return receivedMessageCount.get();
    }

    public double getConsumerTPS() {
        if (0 == this.lastMessageConsumedTimestamp.get() - this.firstMessageConsumedTimestamp.get()) {
            return this.receivedMessageCount.doubleValue() / (1D / 1000);
        } else {
            return this.receivedMessageCount.doubleValue() / ((this.lastMessageConsumedTimestamp.doubleValue() - this.firstMessageConsumedTimestamp.doubleValue()) / 1000D);
        }
    }

    public double getAverageLatency() {
        if (0 == this.receivedMessageCount.get()) {
            log.warn("No messages were received");
            return 0D;
        } else {
            return (this.totalLatency.doubleValue() / 1000D) / this.receivedMessageCount.doubleValue();
        }
    }

    @Override
    public AndesJMSConsumerClientConfiguration getConfig() {
        return this.consumerConfig;
    }
}
