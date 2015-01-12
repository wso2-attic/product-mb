/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.platform.tests.clustering;

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
 *This class includes test cases to test different ack modes for queues
 */
public class DifferentAckModeQueueTestCase extends MBPlatformBaseTest {

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
     * Publish message to a single node and receive from the same node with SESSION_TRANSACTED
     * ack mode
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "SESSION_TRANSACTED ack mode test case for queue")
    public void testSessionTransactedAckModeForQueue() throws Exception {
        // Max number of seconds to run the client
        int maxRunningTime = 80;
        // Expected message count
        int expectedCount = 2000;
        // Number of messages send
        int sendCount = 2000;

        String brokerUrl = getRandomAMQPBrokerUrl();

        AndesClient receivingClient = new AndesClient("receive", brokerUrl,
                "queue:sessionTransactedAckQueue",
                "100", "false",
                String.valueOf(maxRunningTime),
                String.valueOf(expectedCount),
                "1",
                "listener=true,ackMode=0," +
                        "delayBetweenMsg=10," +
                        "stopAfter=" + expectedCount,
                "");
        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", brokerUrl, "queue:sessionTransactedAckQueue", "100",
                "false",
                String.valueOf(maxRunningTime),
                String.valueOf(sendCount), "1",
                "ackMode=0,delayBetweenMsg=0," +
                        "stopAfter=" + sendCount,
                "");
        sendingClient.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient,
                expectedCount,
                maxRunningTime),
                "Message receiving failed.");

        Assert.assertTrue(AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount),
                "Message sending failed.");

        Assert.assertEquals(receivingClient.getReceivedqueueMessagecount(), sendCount,
                "All messages are not received.");

    }


    /**
     * Publish message to a single node and receive from the same node with AUTO_ACKNOWLEDGE
     * ack mode
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "AUTO_ACKNOWLEDGE ack mode test case for queue")
    public void testAutoAcknowledgeModeForQueue() throws Exception {
        // Max number of seconds to run the client
        int maxRunningTime = 80;
        // Expected message count
        int expectedCount = 2000;
        // Number of messages send
        int sendCount = 2000;

        String brokerUrl = getRandomAMQPBrokerUrl();

        AndesClient receivingClient = new AndesClient("receive", brokerUrl,
                "queue:autoAcknowledgeQueue",
                "100", "false",
                String.valueOf(maxRunningTime),
                String.valueOf(expectedCount),
                "1",
                "listener=true,ackMode=1," +
                        "delayBetweenMsg=10," +
                        "stopAfter=" + expectedCount,
                "");
        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", brokerUrl, "queue:autoAcknowledgeQueue", "100",
                "false",
                String.valueOf(maxRunningTime),
                String.valueOf(sendCount), "1",
                "ackMode=1,delayBetweenMsg=0," +
                        "stopAfter=" + sendCount,
                "");
        sendingClient.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient,
                expectedCount,
                maxRunningTime),
                "Message receiving failed.");

        Assert.assertTrue(AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount),
                "Message sending failed.");

        Assert.assertEquals(receivingClient.getReceivedqueueMessagecount(), sendCount,
                "All messages are not received.");

    }

    /**
     * Publish message to a single node and receive from the same node with CLIENT_ACKNOWLEDGE
     * ack mode
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "CLIENT_ACKNOWLEDGE ack mode test case for queue")
    public void testClientAcknowledgeModeForQueue() throws Exception {
        // Max number of seconds to run the client
        int maxRunningTime = 80;
        // Expected message count
        int expectedCount = 2000;
        // Number of messages send
        int sendCount = 2000;

        String brokerUrl = getRandomAMQPBrokerUrl();

        AndesClient receivingClient = new AndesClient("receive", brokerUrl,
                "queue:clientAcknowledgeQueue",
                "100", "false",
                String.valueOf(maxRunningTime),
                String.valueOf(expectedCount),
                "1",
                "listener=true,ackMode=2," +
                        "delayBetweenMsg=10," +
                        "stopAfter=" + expectedCount,
                "");
        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", brokerUrl, "queue:clientAcknowledgeQueue", "100",
                "false",
                String.valueOf(maxRunningTime),
                String.valueOf(sendCount), "1",
                "ackMode=2,delayBetweenMsg=0," +
                        "stopAfter=" + sendCount,
                "");
        sendingClient.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient,
                expectedCount,
                maxRunningTime),
                "Message receiving failed.");

        Assert.assertTrue(AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount),
                "Message sending failed.");

        Assert.assertEquals(receivingClient.getReceivedqueueMessagecount(), sendCount,
                "All messages are not received.");

    }


    /**
     * Publish message to a single node and receive from the same node with DUPS_OK_ACKNOWLEDGE
     * ack mode
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "DUPS_OK_ACKNOWLEDGE ack mode test case for queue")
    public void testDupOkAcknowledgeModeForQueue() throws Exception {
        // Max number of seconds to run the client
        int maxRunningTime = 80;
        // Expected message count
        int expectedCount = 2000;
        // Number of messages send
        int sendCount = 2000;

        String brokerUrl = getRandomAMQPBrokerUrl();

        AndesClient receivingClient = new AndesClient("receive", brokerUrl,
                "queue:dupsOkAcknowledgeQueue",
                "100", "false",
                String.valueOf(maxRunningTime),
                String.valueOf(expectedCount),
                "1",
                "listener=true,ackMode=3," +
                        "delayBetweenMsg=10," +
                        "stopAfter=" + expectedCount,
                "");
        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", brokerUrl, "queue:dupsOkAcknowledgeQueue", "100",
                "false",
                String.valueOf(maxRunningTime),
                String.valueOf(sendCount), "1",
                "ackMode=3,delayBetweenMsg=0," +
                        "stopAfter=" + sendCount,
                "");
        sendingClient.startWorking();

        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient,
                expectedCount,
                maxRunningTime),
                "Message receiving failed.");

        Assert.assertTrue(AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount),
                "Message sending failed.");

        Assert.assertEquals(receivingClient.getReceivedqueueMessagecount(), sendCount,
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

        if (tempAndesAdminClient.getQueueByName("sessionTransactedAckQueue") != null) {
            tempAndesAdminClient.deleteQueue("sessionTransactedAckQueue");
        }

        if (tempAndesAdminClient.getQueueByName("autoAcknowledgeQueue") != null) {
            tempAndesAdminClient.deleteQueue("autoAcknowledgeQueue");
        }

        if (tempAndesAdminClient.getQueueByName("clientAcknowledgeQueue") != null) {
            tempAndesAdminClient.deleteQueue("clientAcknowledgeQueue");
        }

        if (tempAndesAdminClient.getQueueByName("dupsOkAcknowledgeQueue") != null) {
            tempAndesAdminClient.deleteQueue("dupsOkAcknowledgeQueue");
        }
    }

}
