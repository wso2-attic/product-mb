/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.integration.tests.amqp.load;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiThreadedMultipleQueueTopicTestCase {
    private static final long SEND_COUNT = 30000L;
    private static final long ADDITIONAL = 30L;
    private static final long EXPECTED_COUNT = SEND_COUNT + ADDITIONAL;
    private static final int QUEUE_NUMBER_OF_SUBSCRIBERS = 45;
    private static final int QUEUE_NUMBER_OF_PUBLISHERS = 45;
    private static final int TOPIC_NUMBER_OF_SUBSCRIBERS = 45;
    private static final int TOPIC_NUMBER_OF_PUBLISHERS = 15;
    private static final String[] QUEUE_DESTINATIONS = {"Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10","Q11","Q12","Q13","Q14","Q15"};
    private static final String[] TOPIC_DESTINATIONS = {"T1","T2","T3","T4","T5","T6","T7","T8","T9","T10","T11","T12","T13","T14","T15"};
    private List<AndesClient> consumers = new ArrayList<AndesClient>();
    private List<AndesClient> publishers = new ArrayList<AndesClient>();
    @BeforeClass
    public void prepare() {
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb"})
    public void performMultiThreadedMultipleQueueTopicTestCase()
            throws AndesClientException, NamingException, JMSException, IOException {
        for (String queueDestination : QUEUE_DESTINATIONS) {
            // Creating a initial JMS consumer client configuration
            AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, queueDestination);
            // Amount of message to receive
            consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
            // Prints per message
            consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);

            consumers.add(new AndesClient(consumerConfig, QUEUE_NUMBER_OF_SUBSCRIBERS / QUEUE_DESTINATIONS.length));
        }

        for (String topicDestination : TOPIC_DESTINATIONS) {
            // Creating a initial JMS consumer client configuration
            AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, topicDestination);
            // Amount of message to receive
            consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
            // Prints per message
            consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);

            consumers.add(new AndesClient(consumerConfig, TOPIC_NUMBER_OF_SUBSCRIBERS / TOPIC_DESTINATIONS.length));
        }

        for (String queueDestinations : QUEUE_DESTINATIONS) {
            AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, queueDestinations);
            publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
            publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

            publishers.add(new AndesClient(publisherConfig, QUEUE_NUMBER_OF_PUBLISHERS / QUEUE_DESTINATIONS.length));
        }

        for (String topicDestination : TOPIC_DESTINATIONS) {
            AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, topicDestination);
            publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
            publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

            publishers.add(new AndesClient(publisherConfig, TOPIC_NUMBER_OF_PUBLISHERS / TOPIC_DESTINATIONS.length));
        }

        for (AndesClient consumer : consumers) {
            consumer.startClient();
        }

        for (AndesClient publisher : publishers) {
            publisher.startClient();
        }

        for (AndesClient consumer : consumers) {
            AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumer, AndesClientConstants.DEFAULT_RUN_TIME * 3L);
        }

        for (AndesClient publisher : publishers) {
            if (ExchangeType.QUEUE == publisher.getConfig().getExchangeType()) {
                Assert.assertEquals(publisher.getSentMessageCount(), SEND_COUNT * (QUEUE_NUMBER_OF_PUBLISHERS  / QUEUE_DESTINATIONS.length), "Message sending failed for queues for " + publisher.getConfig().getDestinationName());
            } else if (ExchangeType.TOPIC == publisher.getConfig().getExchangeType()) {
                Assert.assertEquals(publisher.getSentMessageCount(), SEND_COUNT * (TOPIC_NUMBER_OF_PUBLISHERS / TOPIC_DESTINATIONS.length), "Message sending failed for topics " + publisher.getConfig().getDestinationName());
            }
        }

        long totalQueueMessagesReceived = 0L;
        long totalTopicMessagesReceived = 0L;
        for (AndesClient consumer : consumers) {
            if (ExchangeType.QUEUE == consumer.getConfig().getExchangeType()) {
                Assert.assertEquals(consumer.getReceivedMessageCount(), (EXPECTED_COUNT-ADDITIONAL) * (TOPIC_NUMBER_OF_SUBSCRIBERS / QUEUE_DESTINATIONS.length), "Message receiving failed " + consumer.getConfig().getDestinationName());
                totalQueueMessagesReceived = totalQueueMessagesReceived + consumer.getReceivedMessageCount();
            } else if (ExchangeType.TOPIC == consumer.getConfig().getExchangeType()) {
                Assert.assertEquals(consumer.getReceivedMessageCount(), EXPECTED_COUNT-ADDITIONAL, "Message receiving failed " + consumer.getConfig().getDestinationName());
                totalTopicMessagesReceived = totalTopicMessagesReceived + consumer.getReceivedMessageCount();
            }
        }

        Assert.assertEquals(totalQueueMessagesReceived, SEND_COUNT * (QUEUE_NUMBER_OF_SUBSCRIBERS / QUEUE_DESTINATIONS.length), "Message receiving failed.");
        Assert.assertEquals(totalQueueMessagesReceived, (EXPECTED_COUNT - ADDITIONAL) * TOPIC_NUMBER_OF_SUBSCRIBERS / TOPIC_DESTINATIONS.length, "Message receiving failed.");





//        Integer queueSendCount = 30000;
//        Integer queueRunTime = 200;
//        Integer queueNumOfSendingThreads = 45;
//        Integer queueNumOfReceivingThreads = 45;
//        int additional = 30;
//
//        Integer topicSendCount = 30000;
//        Integer topicRunTime = 200;
//        Integer topicNumOfSendingThreads = 15;
//        Integer topicNumOfReceivingThreads = 45;
//
//        //wait some more time to see if more messages are received
//        Integer queueExpectedCount = 3 * 2000 * 15 + additional;
//
//        //wait some more time to see if more messages are received
//        Integer topicExpectedCount = topicSendCount + additional;
//
//        AndesClientTemp queueReceivingClient = new AndesClientTemp("receive", "127.0.0.1:5672",
//                "queue:Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15", "100",
//                "false",
//                queueRunTime.toString(), queueExpectedCount.toString(),
//                queueNumOfReceivingThreads.toString(),
//                "listener=true,ackMode=1,delayBetweenMsg=0," +
//                        "stopAfter=" + queueExpectedCount,
//                "");
//
//        queueReceivingClient.startWorking();
//
//
//        AndesClientTemp topicReceivingClient = new AndesClientTemp("receive", "127.0.0.1:5672",
//                "topic:T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15", "100",
//                "false",
//                topicRunTime.toString(), topicExpectedCount.toString(),
//                topicNumOfReceivingThreads.toString(),
//                "listener=true,ackMode=1,delayBetweenMsg=0," +
//                        "stopAfter=" + topicExpectedCount,
//                "");
//
//        topicReceivingClient.startWorking();
//
//
//        AndesClientTemp queueSendingClient = new AndesClientTemp("send", "127.0.0.1:5672",
//                "queue:Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15", "100",
//                "false", queueRunTime.toString(),
//                queueSendCount.toString(), queueNumOfSendingThreads.toString(),
//                "ackMode=1,delayBetweenMsg=0," +
//                        "stopAfter=" + queueSendCount,
//                "");
//
//        queueSendingClient.startWorking();
//
//
//        AndesClientTemp topicSendingClient = new AndesClientTemp("send", "127.0.0.1:5672",
//                "topic:T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15", "100",
//                "false", topicRunTime.toString(),
//                topicSendCount.toString(), topicNumOfSendingThreads.toString(),
//                "ackMode=1,delayBetweenMsg=0," +
//                        "stopAfter=" + topicSendCount,
//                "");
//
//        topicSendingClient.startWorking();
//
//        //let us wait topic message receive time which is larger
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(topicReceivingClient, topicExpectedCount, topicRunTime);
//
//        Assert.assertEquals(queueReceivingClient.getReceivedqueueMessagecount(), queueExpectedCount - additional,
//                "Did not receive expected message count for Queues.");
//        Assert.assertEquals(topicReceivingClient.getReceivedqueueMessagecount(), topicExpectedCount - additional,
//                "Did not receive expected message count for Queues.");
    }
}
