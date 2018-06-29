/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.carbon.mb.migration.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.mb.migration.Main;
import org.wso2.carbon.mb.migration.config.Publisher;
import org.wso2.carbon.mb.migration.config.Queue;
import org.wso2.carbon.mb.migration.config.Receiver;
import org.wso2.carbon.mb.migration.config.Sender;
import org.wso2.carbon.mb.migration.config.Subscriber;
import org.wso2.carbon.mb.migration.config.TestPlan;
import org.wso2.carbon.mb.migration.config.Topic;
import org.wso2.carbon.mb.migration.config.User;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class DataPopulator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataPopulator.class);

    private static final String JNDI_PATH = Main.RESOURCE_DIR + "jndi.properties";

    private final TestPlan testPlan;

    private final Map<String, User> userMap;

    private final InitialContext ctx;

    private final ExecutorService consumerExecutor;

    private final ExecutorService producerExecutor;

    public DataPopulator(TestPlan testPlan, Map<String, User> userMap) throws IOException, NamingException {
        this.testPlan = testPlan;
        this.userMap = userMap;
        Properties properties = new Properties();
        properties.load(new FileReader(JNDI_PATH));
        ctx = new InitialContext(properties);
        int threadCount = Integer.parseInt(System.getProperty("consumer.thread.pool.size", "10"));
        consumerExecutor = Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("Consumer-Pool-%d"));
        producerExecutor = Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("Producer-Pool-%d"));
    }

    public void populateBeforeMigrationData() throws NamingException, JMSException {

        setupQueueReceivers(queue -> queue.getBeforeMigration().getReceivers().getReceiverList());

        setupTopicSubscribers(topic -> topic.getBeforeMigration().getSubscribers().getSubscriberList());

        publishToQueues(queue -> queue.getBeforeMigration().getSenders().getSenderList());

        publishToTopics(topic -> topic.getBeforeMigration().getPublishers().getPublisherList());
    }

    public void populateAfterMigrationData() throws NamingException, JMSException {
        setupQueueReceivers(queue -> queue.getAfterMigration().getReceivers().getReceiverList());

        setupTopicSubscribers(topic -> topic.getAfterMigration().getSubscribers().getSubscriberList());

        publishToQueues(queue -> queue.getAfterMigration().getSenders().getSenderList());

        publishToTopics(topic -> topic.getAfterMigration().getPublishers().getPublisherList());
    }

    private void publishToTopics(Function<Topic, List<Publisher>> publisherProvider) throws JMSException,
                                                                                            NamingException {
        List<Topic> topicList = testPlan.getTopics().getTopicList();

        for (Topic topic: topicList) {
            List<Publisher> publisherList = publisherProvider.apply(topic);
            publishToTopic(topic, publisherList);
        }
    }

    private void publishToQueues(Function<Queue, List<Sender>> senderProvider) throws JMSException,
                                                                                      NamingException {
        List<Queue> queueList = testPlan.getQueues().getQueueList();

        for (Queue queue: queueList) {
            List<Sender> senderList = senderProvider.apply(queue);
            publishToQueue(queue, senderList);
        }
    }

    private void setupTopicSubscribers(Function<Topic, List<Subscriber>> subscriberProvider) throws NamingException,
                                                                                                    JMSException {

        List<Topic> topicList = testPlan.getTopics().getTopicList();

        for (Topic topic: topicList) {
            List<Subscriber> subscriberList = subscriberProvider.apply(topic);
            setupSubscribersForTopic(topic, subscriberList);
        }
    }

    private void setupQueueReceivers(Function<Queue, List<Receiver>> receiverProvider) throws JMSException,
                                                                                              NamingException {
        List<Queue> queueList = testPlan.getQueues().getQueueList();

        for (Queue queue: queueList) {
            List<Receiver> receiverList = receiverProvider.apply(queue);
            setupReceiversForQueue(queue, receiverList);
        }
    }

    private void publishToQueue(Queue queue, List<Sender> senderList) throws NamingException, JMSException {

        for (Sender sender: senderList) {
            JmsConfig queueConfig = new JmsConfig(ctx);
            queueConfig.setConnectionFactoryName("QueueConnectionFactory");
            queueConfig.setDestination(queue.getName());
            String username = sender.getUser();
            User userObj = userMap.get(username);
            queueConfig.setUsername(userObj.getName());
            queueConfig.setPassword(userObj.getPassword());

            JmsQueueSender jmsQueueSender = new JmsQueueSender(queueConfig);

            producerExecutor.submit(() -> {
                try {
                    jmsQueueSender.send(queue.getName(), sender.getMessageCount(), sender.getTps());
                    jmsQueueSender.close();
                } catch (JMSException | NamingException e) {
                    LOGGER.error("Error sending messages to queue {}", queue.getName(), e);
                }
            });
        }
    }

    private void publishToTopic(Topic topic, List<Publisher> publisherList) throws NamingException, JMSException {

        for (Publisher publisher: publisherList) {
            JmsConfig topicConfig = new JmsConfig(ctx);
            topicConfig.setConnectionFactoryName("TopicConnectionFactory");
            topicConfig.setDestination(topic.getName());
            String username = publisher.getUser();
            User userObj = userMap.get(username);
            topicConfig.setUsername(userObj.getName());
            topicConfig.setPassword(userObj.getPassword());

            JmsTopicPublisher jmsTopicPublisher = new JmsTopicPublisher(topicConfig);

            producerExecutor.submit(() -> {
                try {
                    jmsTopicPublisher.send(topic.getName(), publisher.getMessageCount(), publisher.getTps());
                    jmsTopicPublisher.close();
                } catch (JMSException | NamingException e) {
                    LOGGER.error("Error publishing messages to topic {}", topic.getName(), e);
                }
            });
        }
    }

    private void setupReceiversForQueue(Queue queue, List<Receiver> receiverList) throws NamingException, JMSException {

        for (Receiver receiver: receiverList) {
            JmsConfig queueConfig = new JmsConfig(ctx);
            queueConfig.setConnectionFactoryName("QueueConnectionFactory");
            queueConfig.setDestination(queue.getName());
            String username = receiver.getUser();
            User userObj = userMap.get(username);
            queueConfig.setUsername(userObj.getName());
            queueConfig.setPassword(userObj.getPassword());

            JmsQueueReceiver jmsQueueReceiver = new JmsQueueReceiver(queueConfig);

            consumerExecutor.submit(() -> {
                try {
                    jmsQueueReceiver.receive(5000, receiver.getMessageCount());
                    jmsQueueReceiver.close();
                } catch (JMSException e) {
                    LOGGER.error("Error receiving messages for queue {}", queue.getName(), e);
                }
            });
        }
    }

    private void setupSubscribersForTopic(Topic topic, List<Subscriber> subscriberList) throws NamingException,
                                                                                               JMSException {
        for (Subscriber subscriber: subscriberList) {
            JmsConfig topicConfig = new JmsConfig(ctx);
            topicConfig.setConnectionFactoryName("TopicConnectionFactory");
            topicConfig.setDestination(topic.getName());
            String username = subscriber.getUser();
            User userObj = userMap.get(username);
            topicConfig.setUsername(userObj.getName());
            topicConfig.setPassword(userObj.getPassword());
            topicConfig.setSubscriptionId(subscriber.getSubscriptionId());
            JmsTopicSubscriber jmsTopicSubscriber = new JmsTopicSubscriber(topicConfig);

            consumerExecutor.submit(() -> {
                try {
                    jmsTopicSubscriber.receive(5000, subscriber.getMessageCount());
                    jmsTopicSubscriber.close();
                } catch (JMSException e) {
                    LOGGER.error("Error receiving messages for queue {}", topic.getName(), e);
                }
            });
        }
    }

    public void close() throws InterruptedException {
        producerExecutor.shutdown();
        consumerExecutor.shutdown();

        producerExecutor.awaitTermination(3, TimeUnit.MINUTES);
        consumerExecutor.awaitTermination(3, TimeUnit.MINUTES);

    }
}
