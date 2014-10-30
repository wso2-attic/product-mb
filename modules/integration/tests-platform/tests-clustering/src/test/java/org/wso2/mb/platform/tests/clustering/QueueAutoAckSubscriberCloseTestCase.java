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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.queue.QueueMessageReceiver;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import javax.jms.QueueSession;
import javax.xml.xpath.XPathExpressionException;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Load test in MB clustering.
 */
public class QueueAutoAckSubscriberCloseTestCase extends MBPlatformBaseTest {

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
        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);
        super.initAndesAdminClients();
    }

    /**
     * Create 50 subscriptions for a queue and publish one million messages. Then close 10% of the subscribers while
     * messages are retrieving and check if all the messages are received by other subscribers.
     */
    @Test(groups = "wso2.mb", description = "50 subscriptions for a queue and 50 publishers. Then close " +
            "10% of the subscribers ", enabled = true)
    public void performMillionMessageTenPercentSubscriberCloseTestCase() throws XPathExpressionException {
        Integer noOfMessagesToReceiveByClosingSubscribers = 10;
        Integer noOfSubscribersToClose = noOfSubscribers / 10;
        Integer noOfMessagesToExpect = expectedCount - noOfMessagesToReceiveByClosingSubscribers;
        Integer noOfNonClosingSubscribers = noOfSubscribers - noOfSubscribersToClose;
        Integer runTimeForClosingSubscribers = 10; // 10 seconds

        String queueNameArg = "queue:TenPercentSubscriberCloseQueue";

        String randomInstanceKeyForReceiver = getRandomMBInstance();

        AutomationContext tempContextForReceiver = getAutomationContextWithKey(randomInstanceKeyForReceiver);

        String receiverHostInfo = tempContextForReceiver.getInstance().getHosts().get("default") + ":" +
                tempContextForReceiver.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", receiverHostInfo, queueNameArg,
                "100", "false", runTime.toString(), noOfMessagesToExpect.toString(),
                noOfNonClosingSubscribers.toString(), "listener=true,ackMode=1,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");

        AndesClient receivingClosingClient = new AndesClient("receive", receiverHostInfo, queueNameArg,
                "100", "false", runTime.toString(), noOfMessagesToReceiveByClosingSubscribers.toString(),
                noOfSubscribersToClose.toString(), "listener=true,ackMode=1,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");

        receivingClient.startWorking();
        receivingClosingClient.startWorking();

        List<QueueMessageReceiver> queueListeners = receivingClient.getQueueListeners();
        List<QueueMessageReceiver> queueClosingListeners = receivingClosingClient.getQueueListeners();

        log.info("Number of Subscriber ["+queueListeners.size()+"]");
        log.info("Number of Closing Subscriber ["+queueClosingListeners.size()+"]");

        String randomInstanceKeyForSender = getRandomMBInstance();

        AutomationContext tempContextForSender = getAutomationContextWithKey(randomInstanceKeyForSender);

        String senderHostInfo = tempContextForSender.getInstance().getHosts().get("default") + ":" +
                tempContextForSender.getInstance().getPorts().get("amqp");

        AndesClient sendingClient = new AndesClient("send", senderHostInfo, queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        AndesClientUtils.waitUntilAllMessagesReceived(receivingClient, "MillionTenPercentSubscriberCloseQueue", noOfMessagesToExpect,
                runTime);

        AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        AndesClientUtils.waitUntilExactNumberOfMessagesReceived(receivingClosingClient, "MillionTenPercentSubscriberCloseQueue",
                noOfMessagesToReceiveByClosingSubscribers, runTimeForClosingSubscribers);

        receivingClosingClient.shutDownClient();

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount() + receivingClosingClient
                .getReceivedqueueMessagecount();

        log.info("Total Non Closing Subscribers Received Messages ["+receivingClient.getReceivedqueueMessagecount()+"]");
        log.info("Total Closing Subscribers Received Messages ["+receivingClosingClient.getReceivedqueueMessagecount()+"]");
        log.info("Total Received Messages ["+actualReceivedCount+"]");

        assertEquals(actualReceivedCount, sendCount);
        assertEquals(actualReceivedCount, expectedCount);
    }
}
