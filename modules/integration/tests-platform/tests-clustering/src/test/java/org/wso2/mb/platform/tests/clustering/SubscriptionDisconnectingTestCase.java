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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.platform.tests.clustering;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.andes.stub.admin.types.Queue;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.queue.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import static org.testng.Assert.assertTrue;

/**
 * This class includes subscription disconnecting and reconnecting tests
 */
public class SubscriptionDisconnectingTestCase extends MBPlatformBaseTest {

    /**
     * Class Logger
     */
    private static final Log log = LogFactory.getLog(SubscriptionDisconnectingTestCase.class);

    /**
     * Prepare environment for tests.
     *
     * @throws Exception
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);
        super.initAndesAdminClients();
    }

    /**
     * Publish messages to a single node and receive from the same node while reconnecting 4 times.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Same node subscription reconnecting test")
    public void testSameNodeSubscriptionReconnecting() throws Exception {

        int sendCount = 1000;
        int expectedCount = sendCount/4;

        String brokerUrl = getRandomAMQPBrokerUrl();


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(brokerUrl, ExchangeType.QUEUE, "singleQueueSubscription1");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(brokerUrl, ExchangeType.QUEUE, "singleQueueSubscription1");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient1 = new AndesClient(consumerConfig);
        consumerClient1.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(consumerClient1.getReceivedMessageCount(), expectedCount, "Message receiving failed for consumerClient1");

        AndesClient consumerClient2 = new AndesClient(consumerConfig);
        consumerClient2.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(consumerClient2.getReceivedMessageCount(), expectedCount, "Message receiving failed for consumerClient2");

        AndesClient consumerClient3 = new AndesClient(consumerConfig);
        consumerClient3.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient3, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(consumerClient3.getReceivedMessageCount(), expectedCount, "Message receiving failed for consumerClient3");

        AndesClient consumerClient4 = new AndesClient(consumerConfig);
        consumerClient4.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient4, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(consumerClient4.getReceivedMessageCount(), expectedCount, "Message receiving failed for consumerClient4");

        long totalMessagesReceived = consumerClient1.getReceivedMessageCount() + consumerClient2.getReceivedMessageCount() +
                                     consumerClient3.getReceivedMessageCount() + consumerClient4.getReceivedMessageCount();

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(totalMessagesReceived, expectedCount*4, "Message receiving failed.");

//        // Max number of seconds to run the client
//        int maxRunningTime = 20;
//        // Expected message count for a receiver
//        int expectedCount = 250;
//        // Number of messages send
//        int sendCount = 1000;
//
//        String brokerUrl = getRandomAMQPBrokerUrl();
//
//        AndesClient receivingClient1 = new AndesClient("receive", brokerUrl, "queue:singleQueue1",
//                                                       "100", "false",
//                                                       String.valueOf(maxRunningTime),
//                                                       String.valueOf(expectedCount),
//                                                       "1",
//                                                       "listener=true,ackMode=1," +
//                                                       "delayBetweenMsg=0," +
//                                                       "stopAfter=" + expectedCount,
//                                                       "");
//        receivingClient1.startWorking();
//
//        AndesClient sendingClient = new AndesClient("send", brokerUrl, "queue:singleQueue1", "100",
//                                                    "false",
//                                                    String.valueOf(maxRunningTime),
//                                                    String.valueOf(sendCount), "1",
//                                                    "ackMode=1,delayBetweenMsg=0," +
//                                                    "stopAfter=" + sendCount,
//                                                    "");
//        sendingClient.startWorking();
//
//        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1,
//                                                                        expectedCount,
//                                                                        maxRunningTime),
//                          "Message receiving failed for client 1.");
//
//        /** Start 2nd subscriber */
//        AndesClient receivingClient2 = new AndesClient("receive", brokerUrl, "queue:singleQueue1",
//                                                       "100", "false",
//                                                       String.valueOf(maxRunningTime),
//                                                       String.valueOf(expectedCount),
//                                                       "1",
//                                                       "listener=true,ackMode=1," +
//                                                       "delayBetweenMsg=0," +
//                                                       "stopAfter=" + expectedCount,
//                                                       "");
//        receivingClient2.startWorking();
//
//        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2,
//                                                                        expectedCount,
//                                                                        maxRunningTime),
//                          "Message receiving failed for client 2.");
//
//        /** Start 3rd subscriber */
//        AndesClient receivingClient3 = new AndesClient("receive", brokerUrl, "queue:singleQueue1",
//                                                       "100", "false",
//                                                       String.valueOf(maxRunningTime),
//                                                       String.valueOf(expectedCount),
//                                                       "1",
//                                                       "listener=true,ackMode=1," +
//                                                       "delayBetweenMsg=0," +
//                                                       "stopAfter=" + expectedCount,
//                                                       "");
//        receivingClient3.startWorking();
//
//        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3,
//                                                                        expectedCount,
//                                                                        maxRunningTime),
//                          "Message receiving failed for client 3.");
//
//        /** Start 4th subscriber */
//        AndesClient receivingClient4 = new AndesClient("receive", brokerUrl, "queue:singleQueue1",
//                                                       "100", "false",
//                                                       String.valueOf(maxRunningTime),
//                                                       String.valueOf(expectedCount),
//                                                       "1",
//                                                       "listener=true,ackMode=1," +
//                                                       "delayBetweenMsg=0," +
//                                                       "stopAfter=" + expectedCount,
//                                                       "");
//        receivingClient4.startWorking();
//
//        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient4,
//                                                                        expectedCount,
//                                                                        maxRunningTime),
//                          "Message receiving failed for client 4.");
//
//        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount),
//                          "Message sending failed.");
//
//        int allMessagesRecieved = receivingClient1.getReceivedqueueMessagecount()
//                                  + receivingClient2.getReceivedqueueMessagecount()
//                                  + receivingClient3.getReceivedqueueMessagecount()
//                                  + receivingClient4.getReceivedqueueMessagecount();
//        Assert.assertEquals(allMessagesRecieved, sendCount,
//                            "All messages are not received.");
    }

    /**
     * Publish messages to a single node and receive from random nodes while reconnecting 4 times.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Random node subscription reconnecting test")
    public void testDifferentNodeSubscriptionReconnecting() throws Exception {
        int sendCount = 1000;
        int expectedCount = sendCount/4;

        String brokerUrl = getRandomAMQPBrokerUrl();


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(brokerUrl, ExchangeType.QUEUE, "singleQueueSubscription2");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(brokerUrl, ExchangeType.QUEUE, "singleQueueSubscription2");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient1 = new AndesClient(consumerConfig);
        consumerClient1.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(consumerClient1.getReceivedMessageCount(), expectedCount, "Message receiving failed for consumerClient1");

        AndesJMSConsumerClientConfiguration consumerConfig2 = consumerConfig.clone();
        consumerConfig2.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient consumerClient2 = new AndesClient(consumerConfig2);
        consumerClient2.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(consumerClient2.getReceivedMessageCount(), expectedCount, "Message receiving failed for consumerClient2");

        AndesJMSConsumerClientConfiguration consumerConfig3 = consumerConfig.clone();
        consumerConfig3.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient consumerClient3 = new AndesClient(consumerConfig3);
        consumerClient3.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient3, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(consumerClient3.getReceivedMessageCount(), expectedCount, "Message receiving failed for consumerClient3");

        AndesJMSConsumerClientConfiguration consumerConfig4 = consumerConfig.clone();
        consumerConfig4.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient consumerClient4 = new AndesClient(consumerConfig4);
        consumerClient4.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient4, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(consumerClient4.getReceivedMessageCount(), expectedCount, "Message receiving failed for consumerClient4");

        long totalMessagesReceived = consumerClient1.getReceivedMessageCount() + consumerClient2.getReceivedMessageCount() +
                                     consumerClient3.getReceivedMessageCount() + consumerClient4.getReceivedMessageCount();

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(totalMessagesReceived, expectedCount*4, "Message receiving failed.");







//        // Max number of seconds to run the client
//        int maxRunningTime = 20;
//        // Expected message count for a receiver
//        int expectedCount = 250;
//        // Number of messages send
//        int sendCount = 1000;
//
//        String brokerUrl = getRandomAMQPBrokerUrl();
//
//        AndesClient receivingClient1 = new AndesClient("receive", brokerUrl, "queue:singleQueue2",
//                                                       "100", "false", String.valueOf(maxRunningTime),
//                                                       String.valueOf(expectedCount),
//                                                       "1",
//                                                       "listener=true,ackMode=1," +
//                                                       "delayBetweenMsg=0," +
//                                                       "stopAfter=" + expectedCount,
//                                                       "");
//        receivingClient1.startWorking();
//
//        AndesClient sendingClient = new AndesClient("send", brokerUrl, "queue:singleQueue2", "100",
//                                                    "false",
//                                                    String.valueOf(maxRunningTime),
//                                                    String.valueOf(sendCount), "1",
//                                                    "ackMode=1,delayBetweenMsg=0," +
//                                                    "stopAfter=" + sendCount,
//                                                    "");
//        sendingClient.startWorking();
//
//        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1,
//                                                                        expectedCount,
//                                                                        maxRunningTime),
//                          "Message receiving failed for client 1.");
//
//        /** Start 2nd subscriber */
//        AndesClient receivingClient2 = new AndesClient("receive", getRandomAMQPBrokerUrl(), "queue:singleQueue2",
//                                                       "100", "false", String.valueOf(maxRunningTime),
//                                                       String.valueOf(expectedCount),
//                                                       "1",
//                                                       "listener=true,ackMode=1," +
//                                                       "delayBetweenMsg=0," +
//                                                       "stopAfter=" + expectedCount,
//                                                       "");
//        receivingClient2.startWorking();
//
//        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2,
//                                                                        expectedCount,
//                                                                        maxRunningTime),
//                          "Message receiving failed for client 2.");
//
//        /** Start 3rd subscriber */
//        AndesClient receivingClient3 = new AndesClient("receive", getRandomAMQPBrokerUrl(), "queue:singleQueue2",
//                                                       "100", "false", String.valueOf(maxRunningTime),
//                                                       String.valueOf(expectedCount),
//                                                       "1",
//                                                       "listener=true,ackMode=1," +
//                                                       "delayBetweenMsg=0," +
//                                                       "stopAfter=" + expectedCount,
//                                                       "");
//        receivingClient3.startWorking();
//
//        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3,
//                                                                        expectedCount,
//                                                                        maxRunningTime),
//                          "Message receiving failed for client 3.");
//
//        /** Start 4th subscriber */
//        AndesClient receivingClient4 = new AndesClient("receive", getRandomAMQPBrokerUrl(), "queue:singleQueue2",
//                                                       "100", "false", String.valueOf(maxRunningTime),
//                                                       String.valueOf(expectedCount),
//                                                       "1",
//                                                       "listener=true,ackMode=1," +
//                                                       "delayBetweenMsg=0," +
//                                                       "stopAfter=" + expectedCount,
//                                                       "");
//        receivingClient4.startWorking();
//
//        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient4,
//                                                                        expectedCount,
//                                                                        maxRunningTime),
//                          "Message receiving failed for client 4.");
//
//        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount),
//                          "Message sending failed.");
//
//        int allMessagesRecieved = receivingClient1.getReceivedqueueMessagecount()
//                                  + receivingClient2.getReceivedqueueMessagecount()
//                                  + receivingClient3.getReceivedqueueMessagecount()
//                                  + receivingClient4.getReceivedqueueMessagecount();
//        Assert.assertEquals(allMessagesRecieved, sendCount,
//                            "All messages are not received.");
    }

    /**
     * Cleanup after running tests.
     *
     * @throws Exception
     */
    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        String randomInstanceKey = getRandomMBInstance();

        AndesAdminClient tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);

        if (tempAndesAdminClient.getQueueByName("singleQueueSubscription1") != null) {
            tempAndesAdminClient.deleteQueue("singleQueueSubscription1");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueueSubscription2") != null) {
            tempAndesAdminClient.deleteQueue("singleQueueSubscription2");
        }
    }
}