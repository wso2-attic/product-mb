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

    private final AndesJMSConsumerClientConfiguration subscriberConfig;
    private AtomicLong firstMessageConsumedTimestamp;
    private AtomicLong lastMessageConsumedTimestamp;
    private AtomicLong receivedMessageCount;
    private AtomicLong totalLatency;
    private Connection connection;
    private Session session;
    private MessageConsumer receiver;

    public AndesJMSConsumer(AndesJMSConsumerClientConfiguration config)
            throws NamingException, JMSException {
        super(config);
        firstMessageConsumedTimestamp = new AtomicLong();
        lastMessageConsumedTimestamp = new AtomicLong();
        totalLatency = new AtomicLong();
        receivedMessageCount = new AtomicLong();

        this.subscriberConfig = config;
        if (this.subscriberConfig.getExchangeType() == ExchangeType.QUEUE) {
            QueueConnectionFactory connFactory = (QueueConnectionFactory) super.getInitialContext().lookup(AndesClientConstants.CF_NAME);
            QueueConnection queueConnection = connFactory.createQueueConnection();
            queueConnection.start();

            QueueSession queueSession;
            if (this.subscriberConfig.getAcknowledgeMode().getType() == QueueSession.SESSION_TRANSACTED) {
                queueSession = queueConnection.createQueueSession(true, this.subscriberConfig.getAcknowledgeMode().getType());
            } else {
                queueSession = queueConnection.createQueueSession(false, this.subscriberConfig.getAcknowledgeMode().getType());
            }

            Queue queue = (Queue) super.getInitialContext().lookup(this.subscriberConfig.getDestinationName());
            connection = queueConnection;
            session = queueSession;
            receiver = queueSession.createReceiver(queue);
        } else if (this.subscriberConfig.getExchangeType() == ExchangeType.TOPIC) {
            TopicConnectionFactory connFactory = (TopicConnectionFactory) super.getInitialContext().lookup(AndesClientConstants.CF_NAME);
            TopicConnection topicConnection = connFactory.createTopicConnection();
            topicConnection.setClientID(this.subscriberConfig.getSubscriptionID());
            topicConnection.start();
            TopicSession topicSession;
            if (this.subscriberConfig.getAcknowledgeMode().getType() == TopicSession.SESSION_TRANSACTED) {
                topicSession = topicConnection.createTopicSession(true, this.subscriberConfig.getAcknowledgeMode().getType());
            } else {
                topicSession = topicConnection.createTopicSession(false, this.subscriberConfig.getAcknowledgeMode().getType());
            }

            // Send message
            Topic topic = (Topic) super.getInitialContext().lookup(this.subscriberConfig.getDestinationName());

            connection = topicConnection;
            session = topicSession;
            if (this.subscriberConfig.isDurable()) {
                receiver = topicSession.createDurableSubscriber(topic, this.subscriberConfig.getSubscriptionID());
            } else {
                receiver = topicSession.createSubscriber(topic);
            }
        }
    }

    @Override
    public void startClient() throws JMSException, NamingException {
        Thread subscriberThread = new Thread(this);
        subscriberThread.start();
        log.info("Starting consumer | ThreadID : " + subscriberThread.getId());
    }

    @Override
    public synchronized void stopClient() throws JMSException {
        try {
            // TODO : remove duplication if possible
            long threadID = Thread.currentThread().getId();
            log.info("Closing subscriber | ThreadID : " + threadID);
            if (ExchangeType.TOPIC == this.subscriberConfig.getExchangeType()) {
                if (null != this.receiver) {
                    TopicSubscriber topicSubscriber = (TopicSubscriber) this.receiver;
                    topicSubscriber.close();
                }

                if (null != this.session) {
                    TopicSession topicSession = (TopicSession) this.session;
                    topicSession.close();
                }


                if (null != this.connection) {
                    TopicConnection topicConnection = (TopicConnection) this.connection;
                    //topicConnection.stop();
                    topicConnection.close();
                }
            } else if (ExchangeType.QUEUE == this.subscriberConfig.getExchangeType()) {
                if (null != this.receiver) {
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

            log.info("Subscriber closed | ThreadID : " + threadID);

        } catch (JMSException e) {
            log.error("Error in stopping client.", e);
            throw e;
        }
    }

    public synchronized void unSubscribe() throws JMSException {
        try {
            long threadID = Thread.currentThread().getId();
            log.info("Un-subscribing Subscriber | ThreadID : " + threadID);
            this.session.unsubscribe(subscriberConfig.getSubscriptionID());
            log.info("Subscriber Un-Subscribed | ThreadID : " + threadID);
            this.stopClient();

        } catch (JMSException e) {
            log.error("Error in removing subscription(un-subscribing).", e);
            throw e;
        }
    }

    @Override
    public void run() {
        try {
            if (this.subscriberConfig.isAsync()) {
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
                            if (0 == this.receivedMessageCount.get() % this.subscriberConfig.getPrintsPerMessageCount()) {
                                log.info("[DESTINATION RECEIVE] ThreadID:" + threadID + " Destination:" +
                                         this.subscriberConfig.getDestinationName() + " TotalMessageCount:" +
                                         this.receivedMessageCount.get() + " MaximumMessageToReceive:" +
                                         this.subscriberConfig.getMaximumMessagesToReceived() + " Original/Redelivered:" + redelivery);

                            }
                            if (null != this.subscriberConfig.getFilePathToWriteReceivedMessages()) {
                                AndesClientUtils.writeReceivedMessagesToFile(textMessage.getText(), this.subscriberConfig.getFilePathToWriteReceivedMessages());
                            }
                            if (null != this.subscriberConfig.getFilePathToWriteStatistics()) {
                                String statisticsString = Long.toString(currentTimeStamp) + "," + Double.toString(this.getConsumerTPS()) + "," + Double.toString(this.getAverageLatency());
                                AndesClientUtils.writeStatisticsToFile(statisticsString, this.subscriberConfig.getFilePathToWriteStatistics());
                            }
                        }

                        if (0 == this.receivedMessageCount.get() % this.subscriberConfig.getAcknowledgeAfterEachMessageCount()) {
                            if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                                message.acknowledge();
                                log.info("Acknowledging message : " + message.getJMSMessageID());
                            }
                        }

                        //commit get priority
                        if (0 == this.receivedMessageCount.get() % subscriberConfig.getCommitAfterEachMessageCount()) {
                            session.commit();
                            log.info("Committed session");
                        } else if (0 == this.receivedMessageCount.get() % subscriberConfig.getRollbackAfterEachMessageCount()) {
                            session.rollback();
                            log.info("Roll-backed session");
                        }

                        if (this.receivedMessageCount.get() >= subscriberConfig.getUnSubscribeAfterEachMessageCount()) {
                            unSubscribe();
                            break;
                        } else if (this.receivedMessageCount.get() >= subscriberConfig.getMaximumMessagesToReceived()) {
                            stopClient();
                            break;
                        }

                        if (0 < subscriberConfig.getRunningDelay()) {
                            try {
                                Thread.sleep(subscriberConfig.getRunningDelay());
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
                if (0 == this.receivedMessageCount.get() % this.subscriberConfig.getPrintsPerMessageCount()) {
                    log.info("[DESTINATION RECEIVE] ThreadID:" + threadID + " Destination:" +
                             this.subscriberConfig.getDestinationName() + " TotalMessageCount:" +
                             this.receivedMessageCount.get() + " MaximumMessageToReceive:" +
                             this.subscriberConfig.getMaximumMessagesToReceived() + " Original/Redelivered :" + redelivery);

                }
                if (null != this.subscriberConfig.getFilePathToWriteReceivedMessages()) {
                    AndesClientUtils.writeReceivedMessagesToFile(textMessage.getText(), this.subscriberConfig.getFilePathToWriteReceivedMessages());
                }
                if (null != this.subscriberConfig.getFilePathToWriteStatistics()) {
                    String statisticsString = Long.toString(currentTimeStamp) + "," + Double.toString(this.getConsumerTPS()) + "," + Double.toString(this.getAverageLatency());
                    AndesClientUtils.writeStatisticsToFile(statisticsString, this.subscriberConfig.getFilePathToWriteStatistics());
                }
            }

            if (0 == this.receivedMessageCount.get() % this.subscriberConfig.getAcknowledgeAfterEachMessageCount()) {
                if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                    message.acknowledge();
                    log.info("Acknowledging message : " + message.getJMSMessageID());
                }
            }

            //commit get priority
            if (0 == this.receivedMessageCount.get() % subscriberConfig.getCommitAfterEachMessageCount()) {
                session.commit();
                log.info("Committed session");
            } else if (0 == this.receivedMessageCount.get() % subscriberConfig.getRollbackAfterEachMessageCount()) {
                session.rollback();
                log.info("Roll-backed session");
            }

            if (receivedMessageCount.get() >= subscriberConfig.getUnSubscribeAfterEachMessageCount()) {
                Thread unSubscribeThread = new Thread() {
                    @Override
                    public void run() {
                        try {
                            unSubscribe();
                        } catch (JMSException e) {
                            log.error("Error while un-subscribing", e);
                            throw new RuntimeException("JMSException : Error while un-subscribing", e);
                        }
                    }
                };

                unSubscribeThread.start();
            } else if (this.receivedMessageCount.get() >= subscriberConfig.getMaximumMessagesToReceived()) {
                stopClient();
            }

            if (0 < subscriberConfig.getRunningDelay()) {
                try {
                    Thread.sleep(subscriberConfig.getRunningDelay());
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
            return this.receivedMessageCount.doubleValue() / ((this.lastMessageConsumedTimestamp.doubleValue() - this.firstMessageConsumedTimestamp.doubleValue()) / 1000);
        }
    }

    public double getAverageLatency() {
        if (0 == this.receivedMessageCount.get()) {
            log.warn("No messages were received");
            return 0D;
        } else {
            return (this.totalLatency.doubleValue() / 1000) / this.receivedMessageCount.doubleValue();
        }
    }

    @Override
    public AndesJMSConsumerClientConfiguration getConfig() {
        return this.subscriberConfig;
    }
}
