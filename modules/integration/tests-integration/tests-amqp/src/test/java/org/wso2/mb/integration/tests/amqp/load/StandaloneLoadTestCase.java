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
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.queue.QueueMessageReceiver;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.QueueSession;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Load test in standalone MB.
 */
public class StandaloneLoadTestCase extends MBIntegrationBaseTest {

    private Integer sendCount = 100000;
    private Integer runTime = 30 * 15; // 15 minutes
    private Integer noOfSubscribers = 50;
    private Integer noOfPublishers = 50;

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
     * Test Sending million messages through 50 publishers and receive them through 50 subscribers.
     */
    @Test(groups = "wso2.mb", description = "50 publishers and 50 subscribers test case", enabled = true)
    public void performMillionMessageTestCase() {
        String queueNameArg = "queue:MillionQueue";

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), expectedCount.toString(),
                noOfSubscribers.toString(), "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        List<QueueMessageReceiver> queueListeners = receivingClient.getQueueListeners();

        log.info("Number of Subscriber ["+queueListeners.size()+"]");

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        AndesClientUtils.waitUntilAllMessagesReceived(receivingClient, "MillionQueue", expectedCount, runTime);

        AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount();

        log.info("Total Received Messages ["+actualReceivedCount+"]");

        assertEquals(actualReceivedCount, sendCount);
        assertEquals(actualReceivedCount, expectedCount);
    }

    /**
     * Send million messages via 50 publishers and Receive them via 50 AUTO_ACKNOWLEDGE subscribers and 10
     * CLIENT_ACKNOWLEDGE subscribers who receive 10% of the messages and check if AUTO_ACKNOWLEDGE subscribers
     * receive all the messages.
     */
    @Test(groups = "wso2.mb", description = "50 publishers and Receive them via 50 AUTO_ACKNOWLEDGE subscribers and 10 " +
            "CLIENT_ACKNOWLEDGE subscribers who receive 10% of the messages", enabled = true)
    public void performMillionMessageTenPercentReturnTestCase() {
        Integer noOfReturnMessages = sendCount / 10;
        Integer noOfClientAckSubscribers = noOfSubscribers / 10;
        Integer noOfAutoAckSubscribers = noOfSubscribers - noOfClientAckSubscribers;

        String queueNameArg = "queue:MillionTenPercentReturnQueue";

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), expectedCount.toString(),
                noOfAutoAckSubscribers.toString(), "listener=true,ackMode=" + QueueSession.AUTO_ACKNOWLEDGE + ",delayBetweenMsg=0,ackAfterEach=200,stopAfter=" + expectedCount, "");

        AndesClient receivingReturnClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), noOfReturnMessages.toString(),
                noOfClientAckSubscribers.toString(), "listener=true,ackMode=" + QueueSession.CLIENT_ACKNOWLEDGE + ",ackAfterEach=200,delayBetweenMsg=0,stopAfter=" + noOfReturnMessages, "");

        receivingClient.startWorking();
        receivingReturnClient.startWorking();

        List<QueueMessageReceiver> autoAckListeners = receivingClient.getQueueListeners();
        List<QueueMessageReceiver> clientAckListeners = receivingReturnClient.getQueueListeners();
        log.info("Number of AUTO ACK Subscriber ["+autoAckListeners.size()+"]");
        log.info("Number of CLIENT ACK Subscriber ["+clientAckListeners.size()+"]");

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        AndesClientUtils.waitUntilAllMessagesReceived(receivingClient, "MillionTenPercentReturnQueue", expectedCount, runTime);

        AndesClientUtils.waitUntilExactNumberOfMessagesReceived(receivingReturnClient, "MillionTenPercentReturnQueue", noOfReturnMessages, runTime);

        AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount() + receivingReturnClient.getReceivedqueueMessagecount();

        log.info("Total AUTO ACK Subscriber Received Messages ["+receivingClient.getReceivedqueueMessagecount()+"]");
        log.info("Total CLIENT ACK Subscriber Received Messages ["+receivingReturnClient.getReceivedqueueMessagecount()+"]");
        log.info("Total Received Messages ["+actualReceivedCount+"]");

        assertEquals(actualReceivedCount, sendCount);
        assertEquals(actualReceivedCount, expectedCount);
    }

    /**
     * Create 50 subscriptions for a queue and publish one million messages. Then close 10% of the subscribers while
     * messages are retrieving and check if all the messages are received by other subscribers.
     */
    @Test(groups = "wso2.mb", description = "50 subscriptions for a queue and 50 publishers. Then close " +
            "10% of the subscribers ", enabled = true)
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

        List<QueueMessageReceiver> queueListeners = receivingClient.getQueueListeners();
        List<QueueMessageReceiver> queueClosingListeners = receivingClosingClient.getQueueListeners();

        log.info("Number of Subscriber ["+queueListeners.size()+"]");
        log.info("Number of Closing Subscriber ["+queueClosingListeners.size()+"]");

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        AndesClientUtils.waitUntilAllMessagesReceived(receivingClient, "MillionTenPercentSubscriberCloseQueue", noOfMessagesToExpect,
                runTime);

        AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        AndesClientUtils.waitUntilExactNumberOfMessagesReceived(receivingClosingClient, "MillionTenPercentSubscriberCloseQueue",
                noOfMessagesToReceiveByClosingSubscribers, runTimeForClosingSubscribers);

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount() + receivingClosingClient
                .getReceivedqueueMessagecount();

        log.info("Total Non Closing Subscribers Received Messages ["+receivingClient.getReceivedqueueMessagecount()+"]");
        log.info("Total Closing Subscribers Received Messages ["+receivingClosingClient.getReceivedqueueMessagecount()+"]");
        log.info("Total Received Messages ["+actualReceivedCount+"]");

        assertEquals(actualReceivedCount, sendCount);
        assertEquals(actualReceivedCount, expectedCount);
    }
}
