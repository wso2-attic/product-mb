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
 * under the License. and limitations under the License.
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
 * This class includes all order guaranteeing tests
 */
public class OrderGuaranteeTestCase extends MBPlatformBaseTest {

    /**
     * Class Logger
     */
    private static final Log log = LogFactory.getLog(OrderGuaranteeTestCase.class);

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
     * Publish message to a single node and receive from the same node and check for any out of
     * order delivery and message duplication.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Same node ordered delivery test case")
    public void testSameNodeOrderedDelivery() throws Exception {
        // Max number of seconds to run the client
        int maxRunningTime = 20;
        // Expected message count
        int expectedCount = 1000;
        // Number of messages send
        int sendCount = 1000;

        String brokerUrl = getRandomAMQPBrokerUrl();

        AndesClient receivingClient = new AndesClient("receive", brokerUrl, "queue:singleQueue1",
                                                      "100", "false",
                                                      String.valueOf(maxRunningTime),
                                                      String.valueOf(expectedCount),
                                                      "1",
                                                      "listener=true,ackMode=1,delayBetweenMsg=0," +
                                                      "stopAfter=" + expectedCount,
                                                      "");
        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", brokerUrl, "queue:singleQueue1", "100",
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

        Assert.assertTrue(receivingClient.checkIfMessagesAreInOrder(),
                          "Messages did not receive in order.");
        Assert.assertEquals(receivingClient.checkIfMessagesAreDuplicated().size(), 0,
                "Messages are not duplicated.");
    }

    /**
     * Publish message to a single node and receive from another node and check for any out of order
     * delivery and message duplication.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Different node ordered delivery test case")
    public void testDifferentNodeOrderedDelivery() throws Exception {
        // Max number of seconds to run the client
        int maxRunningTime = 20;
        // Expected message count
        int expectedCount = 1000;
        // Number of messages send
        int sendCount = 1000;

        AndesClient receivingClient = new AndesClient("receive", getRandomAMQPBrokerUrl(),
                                                      "queue:singleQueue2",
                                                      "100", "false",
                                                      String.valueOf(maxRunningTime),
                                                      String.valueOf(expectedCount),
                                                      "1",
                                                      "listener=true,ackMode=1,delayBetweenMsg=0," +
                                                      "stopAfter=" + expectedCount,
                                                      "");
        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", getRandomAMQPBrokerUrl(),
                                                    "queue:singleQueue2", "100",
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

        Assert.assertTrue(receivingClient.checkIfMessagesAreInOrder(),
                          "Messages did not receive in order.");
        Assert.assertEquals(receivingClient.checkIfMessagesAreDuplicated().size(), 0,
                "Messages are not duplicated.");
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