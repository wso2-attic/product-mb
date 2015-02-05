package org.wso2.mb.integration.common.clients;

import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSSubscriberClientConfiguration;
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
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.naming.NamingException;

public class AndesJMSSubscriberClient extends AndesJMSClient implements Runnable, MessageListener {
    private static Logger log = Logger.getLogger(AndesJMSSubscriberClient.class);

    private AndesJMSSubscriberClientConfiguration subscriberConfig;
    private Connection connection;
    private Session session;
    private MessageConsumer receiver;

    public AndesJMSSubscriberClient(AndesJMSSubscriberClientConfiguration config)
            throws NamingException, JMSException {
        super(config);

        this.subscriberConfig = (AndesJMSSubscriberClientConfiguration) super.config;
        if (this.subscriberConfig.getExchangeType() == ExchangeType.QUEUE) {
            QueueConnectionFactory connFactory = (QueueConnectionFactory) super.getInitialContext().lookup(AndesClientConstants.CF_NAME);
            QueueConnection queueConnection = connFactory.createQueueConnection();
            queueConnection.start();

            QueueSession queueSession;
            if (this.subscriberConfig.getAcknowledgeMode() == QueueSession.SESSION_TRANSACTED) {
                queueSession = queueConnection.createQueueSession(true, this.subscriberConfig.getAcknowledgeMode());
            } else {
                queueSession = queueConnection.createQueueSession(false, this.subscriberConfig.getAcknowledgeMode());
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
            if (this.subscriberConfig.getAcknowledgeMode() == TopicSession.SESSION_TRANSACTED) {
                topicSession = topicConnection.createTopicSession(true, QueueSession.SESSION_TRANSACTED);
            } else {
                topicSession = topicConnection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);
            }

            // Send message
            Topic topic = (Topic) super.getInitialContext().lookup(this.subscriberConfig.getDestinationName());
            log.info("Starting listening on topic: " + topic);

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
    public void startClient(){
        for (int i = 0; i < this.subscriberConfig.getSubscriberCount(); i++) {
            Thread subscriberThread = new Thread(this);
            subscriberThread.start();
        }
    }

    @Override
    public synchronized void stopClient() throws JMSException {
        try {
            log.info("Closing subscriber");
            if (null != this.receiver) {
                this.receiver.close();
            }

            if (null != this.session) {
                this.session.close();
            }

            if (null != this.connection) {
                this.connection.close();
            }

            log.info("Subscriber closed");

        } catch (JMSException e) {
            log.error("Error in stopping client.", e);
            throw e;
        }
    }

    public synchronized void unSubscribe() throws JMSException {
        try {
            log.info("Un-subscribing Subscriber");
            this.session.unsubscribe(subscriberConfig.getSubscriptionID());
            log.info("Subscriber Un-Subscribed");
            this.stopClient();
            log.info("Closing client");

        } catch (JMSException e) {
            log.error("Error in removing subscription(un-subscribing).", e);
            throw e;
        }
    }

    @Override
    public void run() {
        try {
            if (this.subscriberConfig.isAsync()) {
                this.receiver.setMessageListener(this);
            } else {
                long threadID = Thread.currentThread().getId();
                while (true) {
                    Message message = this.receiver.receive();
//                    log.info("MEEEEEEESSSSAAAAGGGGEEEEE : " + ((TextMessage) message).getText());

                    if (null != message) {
                        // calculating total latency
                        long currentTimeStamp = System.currentTimeMillis();
                        super.totalLatency.set(super.totalLatency.get() + (currentTimeStamp - message.getJMSTimestamp()));
                        // setting timestamps for TPS calculation
                        if (super.firstMessageConsumedTimestamp.get() == 0) {
                            super.firstMessageConsumedTimestamp.set(currentTimeStamp);
                        }

                        super.lastMessageConsumedTimestamp.set(currentTimeStamp);

                        if (message instanceof TextMessage) {
                            super.receivedMessageCount.incrementAndGet();
                            String redelivery;
                            TextMessage textMessage = (TextMessage) message;
                            if (message.getJMSRedelivered()) {
                                redelivery = "REDELIVERED";
                            } else {
                                redelivery = "ORIGINAL";
                            }
    //                        if (super.receivedMessageCount.get() % this.subscriberConfig.getPrintsPerMessageCount() == 0) {
    //                            log.info("[DESTINATION RECEIVE] ThreadID:" + threadID + " destination:" +
    //                                     this.subscriberConfig.getDestinationName() + " totalMessageCount:" +
    //                                     super.receivedMessageCount.get() + " max count:" + this.subscriberConfig.getMaximumMessagesToReceived());
    //                        }
                            if (this.subscriberConfig.getPrintsPerMessageCount() == -1) {
                                log.info("(Count:" + super.receivedMessageCount.get() + " ThreadID:" + threadID
                                         + " Destination:" + this.subscriberConfig.getDestinationName() + ") " + redelivery + " >> " + textMessage.getText());

                                if (null != this.subscriberConfig.getFilePathToWriteReceivedMessages()) {
                                    AndesClientUtils.writeToFile(textMessage.getText(), this.subscriberConfig.getFilePathToWriteReceivedMessages());
                                }
                            }
                        }

                        if (0 == super.receivedMessageCount.get() % this.subscriberConfig.getAcknowledgeAfterEachMessageCount()) {
                            if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                                message.acknowledge();
                            }
                        }

                        //commit get priority
                        if (0 == super.receivedMessageCount.get() % subscriberConfig.getCommitAfterEachMessageCount()) {
                            session.commit();
                            log.info("Committed session");
                        } else if (0 == super.receivedMessageCount.get() % subscriberConfig.getRollbackAfterEachMessageCount()) {
                            session.rollback();
                            log.info("Roll-backed session");
                        }

                        if (super.receivedMessageCount.get() >= subscriberConfig.getUnSubscribeAfterEachMessageCount()) {
                            unSubscribe();
                            break;
                        } else if (super.receivedMessageCount.get() >= subscriberConfig.getMaximumMessagesToReceived()) {
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
            long threadID = Thread.currentThread().getId();
            // calculating total latency
            long currentTimeStamp = System.currentTimeMillis();
            super.totalLatency.set(super.totalLatency.get() + (currentTimeStamp - message.getJMSTimestamp()));
            // setting timestamps for TPS calculation
            if (0 == super.firstMessageConsumedTimestamp.get()) {
                super.firstMessageConsumedTimestamp.set(currentTimeStamp);
            }

            super.lastMessageConsumedTimestamp.set(currentTimeStamp);

            if (message instanceof TextMessage) {
                super.receivedMessageCount.incrementAndGet();
                String redelivery;
                TextMessage textMessage = (TextMessage) message;
                if (message.getJMSRedelivered()) {
                    redelivery = "REDELIVERED";
                } else {
                    redelivery = "ORIGINAL";
                }
                if (0 == super.receivedMessageCount.get() % this.subscriberConfig.getPrintsPerMessageCount()) {
                    log.info("[DESTINATION RECEIVE] ThreadID:" + threadID + " Destination:" +
                             this.subscriberConfig.getDestinationName() + " TotalMessageCount:" +
                             super.receivedMessageCount.get() + " MaximumMessageToReceive:" + this.subscriberConfig.getMaximumMessagesToReceived() + "Original/Redelivered :" + redelivery);
                    if (null != this.subscriberConfig.getFilePathToWriteReceivedMessages()) {
                        AndesClientUtils.writeToFile(textMessage.getText(), this.subscriberConfig.getFilePathToWriteReceivedMessages());
                    }
                }
            }

            if (0 == super.receivedMessageCount.get() % this.subscriberConfig.getAcknowledgeAfterEachMessageCount()) {
                if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                    message.acknowledge();
                }
            }

            //commit get priority
            if (0 == super.receivedMessageCount.get() % subscriberConfig.getCommitAfterEachMessageCount()) {
                session.commit();
                log.info("Committed session");
            } else if (0 == super.receivedMessageCount.get() % subscriberConfig.getRollbackAfterEachMessageCount()) {
                session.rollback();
                log.info("Roll-backed session");
            }

            if (receivedMessageCount.get() >= subscriberConfig.getUnSubscribeAfterEachMessageCount()) {
                Thread unSubscribeThread = new Thread(){
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
            } else if (receivedMessageCount.get() >= subscriberConfig.getMaximumMessagesToReceived()) {
                Thread stopClientThread = new Thread(){
                    @Override
                    public void run() {
                        try {
                            stopClient();
                        } catch (JMSException e) {
                            log.error("Error while stopping the client", e);
                            throw new RuntimeException("JMSException : Error while stopping the client", e);
                        }
                    }
                };

                stopClientThread.start();
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
}
