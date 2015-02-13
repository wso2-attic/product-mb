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
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.queue.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

/**
 * This class tests broker with multiple publisher and subscribers
 */
public class MultipleSubscriberMultiplePublisherTestCase extends MBPlatformBaseTest {

    /**
     * Class Logger
     */
    private static final Log log =
            LogFactory.getLog(MultipleSubscriberMultiplePublisherTestCase.class);

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
     * Multiple subscribers and publishers in same node for a single queue
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Same node single queue multiple subscriber " +
                                            "publisher test case")
    public void testSameNodeSingleQueueMultipleSubscriberPublisher() throws Exception {
        // Max number of seconds to run the client
        int maxRunningTime = 20;
        // Expected message count
        int expectedCount = 250;
        // Number of messages send
        int sendCount = 250;

        String brokerURL = getRandomAMQPBrokerUrl();
        AndesClient receivingClient1 = new AndesClient("receive", brokerURL,
                                                       "queue:singleQueue1",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        AndesClient receivingClient2 = new AndesClient("receive", brokerURL,
                                                       "queue:singleQueue1",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        AndesClient receivingClient3 = new AndesClient("receive", brokerURL,
                                                       "queue:singleQueue1",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        AndesClient receivingClient4 = new AndesClient("receive", brokerURL,
                                                       "queue:singleQueue1",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient1.startWorking();
        receivingClient2.startWorking();
        receivingClient3.startWorking();
        receivingClient4.startWorking();

        AndesClient sendingClient1 = new AndesClient("send", brokerURL,
                                                     "queue:singleQueue1", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient1.startWorking();
        AndesClient sendingClient2 = new AndesClient("send", brokerURL,
                                                     "queue:singleQueue1", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient2.startWorking();
        AndesClient sendingClient3 = new AndesClient("send", brokerURL,
                                                     "queue:singleQueue1", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient3.startWorking();
        AndesClient sendingClient4 = new AndesClient("send", brokerURL,
                                                     "queue:singleQueue1", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient4.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 1.");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 2.");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 3.");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient4,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 4.");

        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 1.");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 2.");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 3.");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 4.");

        int receiveCount = receivingClient1.getReceivedqueueMessagecount()
                           + receivingClient2.getReceivedqueueMessagecount()
                           + receivingClient3.getReceivedqueueMessagecount()
                           + receivingClient4.getReceivedqueueMessagecount();
        Assert.assertEquals(receiveCount, sendCount * 4,
                            "All messages are not received.");
    }

    /**
     * Multiple subscribers and publishers in Multiple node for a single queue
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Multiple node single queue multiple subscriber " +
                                            "publisher test case")
    public void testMultiNodeSingleQueueMultipleSubscriberPublisher() throws Exception {
        // Max number of seconds to run the client
        int maxRunningTime = 20;
        // Expected message count
        int expectedCount = 250;
        // Number of messages send
        int sendCount = 250;

        AndesClient receivingClient1 = new AndesClient("receive", getRandomAMQPBrokerUrl(),
                                                       "queue:singleQueue2",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        AndesClient receivingClient2 = new AndesClient("receive", getRandomAMQPBrokerUrl(),
                                                       "queue:singleQueue2",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        AndesClient receivingClient3 = new AndesClient("receive", getRandomAMQPBrokerUrl(),
                                                       "queue:singleQueue2",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        AndesClient receivingClient4 = new AndesClient("receive", getRandomAMQPBrokerUrl(),
                                                       "queue:singleQueue2",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient1.startWorking();
        receivingClient2.startWorking();
        receivingClient3.startWorking();
        receivingClient4.startWorking();

        AndesClient sendingClient1 = new AndesClient("send", getRandomAMQPBrokerUrl(),
                                                     "queue:singleQueue2", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient1.startWorking();
        AndesClient sendingClient2 = new AndesClient("send", getRandomAMQPBrokerUrl(),
                                                     "queue:singleQueue2", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient2.startWorking();
        AndesClient sendingClient3 = new AndesClient("send", getRandomAMQPBrokerUrl(),
                                                     "queue:singleQueue2", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient3.startWorking();
        AndesClient sendingClient4 = new AndesClient("send", getRandomAMQPBrokerUrl(),
                                                     "queue:singleQueue2", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient4.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 1.");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 2.");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 3.");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient4,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 4.");

        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 1.");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 2.");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 3.");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 4.");

        int receiveCount = receivingClient1.getReceivedqueueMessagecount()
                           + receivingClient2.getReceivedqueueMessagecount()
                           + receivingClient3.getReceivedqueueMessagecount()
                           + receivingClient4.getReceivedqueueMessagecount();
        Assert.assertEquals(receiveCount, sendCount * 4,
                            "All messages are not received.");
    }

    /**
     * Multiple subscribers and publishers in Multiple node for Multiple queues
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Multiple node Multiple queue multiple subscriber " +
                                            "publisher test case")
    public void testMultiNodeMultipleQueueMultipleSubscriberPublisher() throws Exception {
        // Max number of seconds to run the client
        int maxRunningTime = 20;
        // Expected message count
        int expectedCount = 250;
        // Number of messages send
        int sendCount = 250;

        AndesClient receivingClient1 = new AndesClient("receive", getRandomAMQPBrokerUrl(),
                                                       "queue:singleQueue3",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        AndesClient receivingClient2 = new AndesClient("receive", getRandomAMQPBrokerUrl(),
                                                       "queue:singleQueue4",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        AndesClient receivingClient3 = new AndesClient("receive", getRandomAMQPBrokerUrl(),
                                                       "queue:singleQueue5",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        AndesClient receivingClient4 = new AndesClient("receive", getRandomAMQPBrokerUrl(),
                                                       "queue:singleQueue6",
                                                       "100", "false",
                                                       String.valueOf(maxRunningTime),
                                                       String.valueOf(expectedCount),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount,
                                                       "");
        receivingClient1.startWorking();
        receivingClient2.startWorking();
        receivingClient3.startWorking();
        receivingClient4.startWorking();

        AndesClient sendingClient1 = new AndesClient("send", getRandomAMQPBrokerUrl(),
                                                     "queue:singleQueue3", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient1.startWorking();
        AndesClient sendingClient2 = new AndesClient("send", getRandomAMQPBrokerUrl(),
                                                     "queue:singleQueue4", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient2.startWorking();
        AndesClient sendingClient3 = new AndesClient("send", getRandomAMQPBrokerUrl(),
                                                     "queue:singleQueue5", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient3.startWorking();
        AndesClient sendingClient4 = new AndesClient("send", getRandomAMQPBrokerUrl(),
                                                     "queue:singleQueue6", "100",
                                                     "false",
                                                     String.valueOf(maxRunningTime),
                                                     String.valueOf(sendCount), "1",
                                                     "ackMode=1,delayBetweenMsg=0," +
                                                     "stopAfter=" + sendCount,
                                                     "");
        sendingClient4.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 1.");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 2.");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 3.");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient4,
                                                                        expectedCount,
                                                                        maxRunningTime),
                          "Message receiving failed for client 4.");

        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 1.");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 2.");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 3.");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                          "Message sending failed for client 4.");

        int receiveCount = receivingClient1.getReceivedqueueMessagecount()
                           + receivingClient2.getReceivedqueueMessagecount()
                           + receivingClient3.getReceivedqueueMessagecount()
                           + receivingClient4.getReceivedqueueMessagecount();
        Assert.assertEquals(receiveCount, sendCount * 4,
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

        if (tempAndesAdminClient.getQueueByName("singleQueue3") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue3");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueue4") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue4");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueue5") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue5");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueue6") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue6");
        }
    }
}