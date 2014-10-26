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
 * Load test for standalone MB.
 */
public class QueueAckMixTestCase extends MBIntegrationBaseTest {

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
        restartServer();
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * Send million messages and Receive them via AUTO_ACKNOWLEDGE subscribers and CLIENT_ACKNOWLEDGE subscribers and
     * check if AUTO_ACKNOWLEDGE subscribers receive all the messages.
     */
    @Test(groups = "wso2.mb", description = "Send million messages and Receive them via AUTO_ACKNOWLEDGE subscribers " +
            "and CLIENT_ACKNOWLEDGE", enabled = true)
    public void performMillionMessageTenPercentReturnTestCase() {
        Integer noOfReturnMessages = sendCount / 100;

        Integer noOfClientAckSubscribers = 1;
        Integer noOfAutoAckSubscribers = noOfSubscribers - noOfClientAckSubscribers;

        String queueNameArg = "queue:MillionTenPercentReturnQueue";

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), expectedCount.toString(),
                noOfAutoAckSubscribers.toString(), "listener=true,ackMode=" + QueueSession.AUTO_ACKNOWLEDGE + "," +
                "delayBetweenMsg=0,ackAfterEach=200,stopAfter=" + expectedCount, "");

        AndesClient receivingReturnClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), noOfReturnMessages.toString(),
                noOfClientAckSubscribers.toString(), "listener=true,ackMode=" + QueueSession.CLIENT_ACKNOWLEDGE + "," +
                "ackAfterEach=100000,delayBetweenMsg=0,stopAfter=" + noOfReturnMessages, "");

        receivingClient.startWorking();
        receivingReturnClient.startWorking();

        List<QueueMessageReceiver> autoAckListeners = receivingClient.getQueueListeners();
        List<QueueMessageReceiver> clientAckListeners = receivingReturnClient.getQueueListeners();
        log.info("Number of AUTO ACK Subscriber [" + autoAckListeners.size() + "]");
        log.info("Number of CLIENT ACK Subscriber [" + clientAckListeners.size() + "]");

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        AndesClientUtils.waitUntilAllMessagesReceived(receivingClient, "MillionTenPercentReturnQueue", expectedCount,
                runTime);

        AndesClientUtils.waitUntilExactNumberOfMessagesReceived(receivingReturnClient,
                "MillionTenPercentReturnQueue", noOfReturnMessages, runTime);

        AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount() + receivingReturnClient
                .getReceivedqueueMessagecount();

        log.info("Total AUTO ACK Subscriber Received Messages [" + receivingClient.getReceivedqueueMessagecount() +
                "]");
        log.info("Total CLIENT ACK Subscriber Received Messages [" + receivingReturnClient
                .getReceivedqueueMessagecount() + "]");
        log.info("Total Received Messages [" + actualReceivedCount + "]");

        assertEquals(receivingClient.getReceivedqueueMessagecount(), sendCount.intValue(),
                "Did not receive expected message count.");
    }
}
