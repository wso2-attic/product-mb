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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.operations.queue.QueueMessageReceiver;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Load test for standalone MB.
 */
public class QueueAutoAckTestCase extends MBIntegrationBaseTest {

    private Integer sendCount = 100000;
    private Integer runTime = 30 * 15; // 15 minutes
    private Integer noOfSubscribers = 7;
    private Integer noOfPublishers = 7;

    // Greater than send count to see if more than the sent amount is received
    private Integer expectedCount = sendCount;

    /**
     * Initialize the test as super tenant user.
     *
     * @throws Exception
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * Test Sending million messages through [noOfPublishers] publishers and receive them through [noOfSubscribers]
     * subscribers.
     */
    @Test(groups = "wso2.mb", description = "Million message test case", enabled = true)
    public void performMillionMessageTestCase() {
        String queueNameArg = "queue:MillionQueue";

        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), expectedCount.toString(),
                noOfSubscribers.toString(), "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        List<QueueMessageReceiver> queueListeners = receivingClient.getQueueListeners();

        log.info("Number of Subscriber [" + queueListeners.size() + "]");

        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);

        AndesClientUtilsTemp.waitUntilAllMessagesReceived(receivingClient, "MillionQueue", expectedCount, runTime);

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount();

        assertEquals(actualReceivedCount, sendCount, "Did not receive expected message count.");
    }
}
