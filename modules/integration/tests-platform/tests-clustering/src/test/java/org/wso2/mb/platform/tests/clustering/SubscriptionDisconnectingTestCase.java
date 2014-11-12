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
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.queue.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

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
        // Max number of seconds to run the client
        int maxRunningTime = 20;
        // Expected message count for a receiver
        int expectedCount = 250;
        // Number of messages send
        int sendCount = 1000;

        String brokerUrl = getRandomAMQPBrokerUrl();

        AndesClient receivingClient1 = new AndesClient("receive", brokerUrl, "queue:singleQueue1",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient1.startWorking();

        AndesClient sendingClient = new AndesClient("send", brokerUrl, "queue:singleQueue1", "100",
                                                    "false",
                                                    String.valueOf(maxRunningTime),
                                                    String.valueOf(sendCount), "1",
                                                    "ackMode=1,delayBetweenMsg=0," +
                                                    "stopAfter=" + sendCount,
                                                    "");
        sendingClient.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 1.");

        /** Start 2nd subscriber */
        AndesClient receivingClient2 = new AndesClient("receive", brokerUrl, "queue:singleQueue1",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient2.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 2.");

        /** Start 3rd subscriber */
        AndesClient receivingClient3 = new AndesClient("receive", brokerUrl, "queue:singleQueue1",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient3.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 3.");

        /** Start 4th subscriber */
        AndesClient receivingClient4 = new AndesClient("receive", brokerUrl, "queue:singleQueue1",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient4.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient4,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 4.");

        Assert.assertTrue(AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount),
                          "Message sending failed.");

        int allMessagesRecieved = receivingClient1.getReceivedqueueMessagecount()
                                  + receivingClient2.getReceivedqueueMessagecount()
                                  + receivingClient3.getReceivedqueueMessagecount()
                                  + receivingClient4.getReceivedqueueMessagecount();
        Assert.assertEquals(allMessagesRecieved, sendCount,
                            "All messages are not received.");
    }

    /**
     * Publish messages to a single node and receive from random nodes while reconnecting 4 times.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Random node subscription reconnecting test")
    public void testDifferentNodeSubscriptionReconnecting() throws Exception {
        // Max number of seconds to run the client
        int maxRunningTime = 20;
        // Expected message count for a receiver
        int expectedCount = 250;
        // Number of messages send
        int sendCount = 1000;

        String brokerUrl = getRandomAMQPBrokerUrl();

        AndesClient receivingClient1 = new AndesClient("receive", brokerUrl, "queue:singleQueue2",
                                                       "100", "false", String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient1.startWorking();

        AndesClient sendingClient = new AndesClient("send", brokerUrl, "queue:singleQueue2", "100",
                                                    "false",
                                                    String.valueOf(maxRunningTime),
                                                    String.valueOf(sendCount), "1",
                                                    "ackMode=1,delayBetweenMsg=0," +
                                                    "stopAfter=" + sendCount,
                                                    "");
        sendingClient.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 1.");

        /** Start 2nd subscriber */
        AndesClient receivingClient2 = new AndesClient("receive", getRandomAMQPBrokerUrl(), "queue:singleQueue2",
                                                       "100", "false", String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient2.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 2.");

        /** Start 3rd subscriber */
        AndesClient receivingClient3 = new AndesClient("receive", getRandomAMQPBrokerUrl(), "queue:singleQueue2",
                                                       "100", "false", String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient3.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 3.");

        /** Start 4th subscriber */
        AndesClient receivingClient4 = new AndesClient("receive", getRandomAMQPBrokerUrl(), "queue:singleQueue2",
                                                       "100", "false", String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient4.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient4,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 4.");

        Assert.assertTrue(AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount),
                          "Message sending failed.");

        int allMessagesRecieved = receivingClient1.getReceivedqueueMessagecount()
                                  + receivingClient2.getReceivedqueueMessagecount()
                                  + receivingClient3.getReceivedqueueMessagecount()
                                  + receivingClient4.getReceivedqueueMessagecount();
        Assert.assertEquals(allMessagesRecieved, sendCount,
                            "All messages are not received.");
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

        if (tempAndesAdminClient.getQueueByName("singleQueue1") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue1");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueue2") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue2");
        }
    }
}