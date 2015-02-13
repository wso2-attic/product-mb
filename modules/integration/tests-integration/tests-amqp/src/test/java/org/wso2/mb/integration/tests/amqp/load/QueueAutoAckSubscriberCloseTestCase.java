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
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.operations.queue.QueueMessageReceiver;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import java.util.List;

/**
 * Load test for standalone MB.
 */
public class QueueAutoAckSubscriberCloseTestCase extends MBIntegrationBaseTest {

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
     * Create 50 subscriptions for a queue and publish one million messages. Then close 10% of the subscribers while
     * messages are retrieving and check if all the messages are received by other subscribers.
     */
    @Test(groups = "wso2.mb", description = "50 subscriptions for a queue and 50 publishers. Then close " +
            "10% of the subscribers ", enabled = true)
    public void performMillionMessageTenPercentSubscriberCloseTestCase() {
        Integer noOfMessagesToReceiveByClosingSubscribers = 1000;
        Integer noOfSubscribersToClose = 1;
        Integer noOfMessagesToExpect = expectedCount - noOfMessagesToReceiveByClosingSubscribers;
        Integer noOfNonClosingSubscribers = noOfSubscribers - noOfSubscribersToClose;

        String queueNameArg = "queue:MillionTenPercentSubscriberCloseQueue";

        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), noOfMessagesToExpect.toString(),
                noOfNonClosingSubscribers.toString(), "listener=true,ackMode=1,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");

        AndesClientTemp receivingClosingClient = new AndesClientTemp("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), noOfMessagesToReceiveByClosingSubscribers.toString(),
                noOfSubscribersToClose.toString(), "listener=true,ackMode=1,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");

        receivingClient.startWorking();
        receivingClosingClient.startWorking();

        List<QueueMessageReceiver> queueListeners = receivingClient.getQueueListeners();
        List<QueueMessageReceiver> queueClosingListeners = receivingClosingClient.getQueueListeners();

        log.info("Number of Subscriber [" + queueListeners.size() + "]");
        log.info("Number of Closing Subscriber [" + queueClosingListeners.size() + "]");

        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        AndesClientUtilsTemp.waitUntilAllMessagesReceived(receivingClient, "MillionTenPercentSubscriberCloseQueue",
                                                          noOfMessagesToExpect,
                                                          runTime);

        AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);

        AndesClientUtilsTemp.waitUntilExactNumberOfMessagesReceived(receivingClosingClient,
                "MillionTenPercentSubscriberCloseQueue",
                noOfMessagesToReceiveByClosingSubscribers, runTime);

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount() + receivingClosingClient
                .getReceivedqueueMessagecount();

        log.info("Total Non Closing Subscribers Received Messages [" + receivingClient.getReceivedqueueMessagecount()
                + "]");
        log.info("Total Closing Subscribers Received Messages [" + receivingClosingClient
                .getReceivedqueueMessagecount() + "]");
        log.info("Total Received Messages [" + actualReceivedCount + "]");

        Assert.assertTrue(actualReceivedCount >= sendCount, "Received only " + actualReceivedCount + " messages.");
    }
}
