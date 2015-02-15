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
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 1. define 15 queues
 * 2. send 30000 messages to 15 queues (2000 to each) by 45 threads.
 * 3. receive messages from 15 queues by 45 threads
 * 4. verify that all messages are received and no more messages are received
 */
public class MultiThreadedMultipleQueueTestCase extends MBIntegrationBaseTest {
    private static final long SEND_COUNT = 30000L;
    private static final long ADDITIONAL = 30L;
    private static final long EXPECTED_COUNT = SEND_COUNT + ADDITIONAL;
    private static final int NUMBER_OF_SUBSCRIBERS = 45;
    private static final int NUMBER_OF_PUBLISHERS = 45;
    private static final String[] DESTINATIONS = {"Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10","Q11","Q12","Q13","Q14","Q15"};
    private List<AndesClient> consumers = new ArrayList<AndesClient>();
    private List<AndesClient> publishers = new ArrayList<AndesClient>();

    @BeforeClass
    public void prepare() throws Exception {
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performMultiThreadedMultipleQueueTestCase()
            throws AndesClientException, NamingException, JMSException, IOException {

        for (String DESTINATION : DESTINATIONS) {
            // Creating a initial JMS consumer client configuration
            AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, DESTINATION);
            // Amount of message to receive
            consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
            // Prints per message
            consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);

            consumers.add(new AndesClient(consumerConfig, NUMBER_OF_SUBSCRIBERS / DESTINATIONS.length));
        }

        for (String DESTINATION : DESTINATIONS) {
            AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, DESTINATION);
            publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
            publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

            publishers.add(new AndesClient(publisherConfig, NUMBER_OF_PUBLISHERS / DESTINATIONS.length));
        }

        for (AndesClient consumer : consumers) {
            consumer.startClient();
        }

        for (AndesClient publisher : publishers) {
            publisher.startClient();
        }

        for (AndesClient consumer : consumers) {
            AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumer, AndesClientConstants.DEFAULT_RUN_TIME * 2L);
        }

        for (AndesClient publisher : publishers) {
            Assert.assertEquals(publisher.getSentMessageCount(), SEND_COUNT * (NUMBER_OF_PUBLISHERS / DESTINATIONS.length), "Message sending failed");
        }

        long totalMessagesReceived = 0L;
        for (AndesClient consumer : consumers) {
            Assert.assertEquals(consumer.getReceivedMessageCount(), EXPECTED_COUNT * (NUMBER_OF_SUBSCRIBERS / DESTINATIONS.length), "Message receiving failed.");
            totalMessagesReceived = totalMessagesReceived + consumer.getReceivedMessageCount();
        }

        Assert.assertEquals(totalMessagesReceived, EXPECTED_COUNT - ADDITIONAL, "Message receiving failed.");


//        // Creating a initial JMS consumer client configuration
//        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "singleQueue");
//        // Amount of message to receive
//        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
//        // Prints per message
//        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT/10L);
//
//        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "singleQueue");
//        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
//        publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);
//
//        AndesClient consumerClient = new AndesClient(consumerConfig, NUMBER_OF_SUBSCRIBERS);
//        consumerClient.startClient();
//
//        AndesClient publisherClient = new AndesClient(publisherConfig, NUMBER_OF_PUBLISHERS);
//        publisherClient.startClient();
//
//        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
//
//        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT * NUMBER_OF_SUBSCRIBERS, "Message sending failed");
//
//        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT* NUMBER_OF_SUBSCRIBERS, "Message receiving failed.");







//        Integer sendCount = 30000;
//        Integer runTime = 200;
//        Integer numOfSendingThreads = 45;
//        Integer numOfReceivingThreads = 45;
//        int additional = 30;
//
//        //wait some more time to see if more messages are received
//        Integer expectedCount = sendCount + additional;
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672",
//                "queue:Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15", "100",
//                "false",
//                runTime.toString(), expectedCount.toString(),
//                numOfReceivingThreads.toString(),
//                "listener=true,ackMode=1,delayBetweenMsg=0," +
//                        "stopAfter=" + expectedCount,
//                "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672",
//                "queue:Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15", "100",
//                "false", runTime.toString(),
//                sendCount.toString(), numOfSendingThreads.toString(),
//                "ackMode=1,delayBetweenMsg=0," +
//                        "stopAfter=" + sendCount,
//                "");
//
//        sendingClient.startWorking();
//
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        Assert.assertEquals(receivingClient.getReceivedqueueMessagecount(), expectedCount - additional,
//                "Did not receive expected message count.");
    }
}
