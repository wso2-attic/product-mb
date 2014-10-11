/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.mb.integration.tests.amqp.load;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.QueueSession;
import java.io.*;

import static org.testng.Assert.assertEquals;

/**
 * This class contains tests for sending and receiving one million messages.
 */
public class MillionMessagesTestCase extends MBIntegrationBaseTest {

    private Integer sendCount = 1000000;
    private Integer runTime = 60 * 15; // 15 minutes
    private Integer noOfSubscribers = 50;
    private Integer noOfPublishers = 50;

    // Greater than send count to see if more than the sent amount is received
    private Integer expectedCount = sendCount + 10;

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
     * Test Sending million messages through 50 publishers and receive them through 50 subscribers.
     */
    @Test(groups = "wso2.mb", description = "Message content validation test case")
    public void performMillionMessageTestCase() {
        String queueNameArg = "queue:MillionQueue";

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), expectedCount.toString(),
                noOfSubscribers.toString(), "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount();

        assertEquals((receiveSuccess && sendSuccess), true);
        assertEquals(actualReceivedCount, sendCount);
    }

    /**
     * Send million messages via 50 publishers and Receive them via 50 AUTO_ACKNOWLEDGE subscribers and 10
     * CLIENT_ACKNOWLEDGE subscribers who receive 10% of the messages and check if AUTO_ACKNOWLEDGE subscribers
     * receive all the messages.
     */
    @Test(groups = "wso2.mb", description = "Message content validation test case", enabled = false)
    public void performMillionMessageTenPercentReturnTestCase() {
        Integer noOfReturnMessages = sendCount / 10;
        Integer noOfReturnSubscribers = noOfSubscribers / 10;

        String queueNameArg = "queue:MillionTenPercentReturnQueue";

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), expectedCount.toString(),
                noOfSubscribers.toString(), "listener=true,ackMode=" + QueueSession.AUTO_ACKNOWLEDGE + ",delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        AndesClient receivingReturnClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), noOfReturnMessages.toString(),
                noOfReturnSubscribers.toString(), "listener=true,ackMode=" + QueueSession.CLIENT_ACKNOWLEDGE + ",delayBetweenMsg=0,stopAfter=" + noOfReturnMessages, "");

        receivingClient.startWorking();
        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        boolean returnSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingReturnClient, noOfReturnMessages, runTime);

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount();

        assertEquals((receiveSuccess && sendSuccess && returnSuccess), true);
        assertEquals(actualReceivedCount, sendCount);
    }

    /**
     * Create 50 subscriptions for a queue and publish one million messages. Then close 10% of the subscribers while
     * messages are retrieving and check if all the messages are received by other subscribers.
     */
    @Test(groups = "wso2.mb", description = "Message content validation test case", enabled = false)
    public void performMillionMessageTenPercentSubscriberCloseTestCase() {
        Integer noOfMessagesToReceiveByClosingSubscribers = 10;
        Integer noOfSubscribersToClose = noOfSubscribers / 10;
        Integer noOfMessagesToExpect = expectedCount - noOfMessagesToReceiveByClosingSubscribers;
        Integer noOfNonClosingSubscribers = noOfSubscribers - noOfSubscribersToClose;
        Integer runTimeForClosingSubscribers = 10; // 10 seconds

        String queueNameArg = "queue:MillionTenPercentSubscriberCloseQueue";

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), noOfMessagesToExpect.toString(),
                noOfNonClosingSubscribers.toString(), "listener=true,ackMode=1,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");

        AndesClient receivingClosingClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), noOfMessagesToReceiveByClosingSubscribers.toString(),
                noOfSubscribersToClose.toString(), "listener=true,ackMode=1,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");

        receivingClient.startWorking();
        receivingClosingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, noOfMessagesToExpect,
                runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        boolean closedReceiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClosingClient,
                noOfMessagesToReceiveByClosingSubscribers, runTimeForClosingSubscribers);

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount() + receivingClosingClient
                .getReceivedqueueMessagecount();
        ;

        assertEquals((receiveSuccess && sendSuccess && closedReceiveSuccess), true);
        assertEquals(actualReceivedCount, sendCount);
    }
}
