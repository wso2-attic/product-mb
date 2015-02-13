/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.mb.integration.tests.jms.selectors;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;
import org.wso2.mb.integration.tests.JMSTestConstants;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SelectorsTestCase extends MBIntegrationBaseTest {
    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
    }

    /**
     * 1. Subscribe to a queue with selectors.
     * 2. Send messages without jms type as configured
     * 3. Verify that 0 messages received by receiver
     */
    @Test(groups = "wso2.mb", description = "send-receive test case with jms selectors without conforming messages")
    public void performQueueSendWithoutConformingMsgTestCase() {

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
                                                      JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(),
                                                      "0", "1", "listener=true,ackMode=1,delayBetweenMsg=" +
                                                                JMSTestConstants.STANDARD_DELAY_BETWEEN_MESSAGES +
                                                                ",stopAfter=" + 0 + ",jMSSelector=JMSType='AAA'", "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
                                                    JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(),
                                                    JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT.toString(), "1",
                                                    "ackMode=1,delayBetweenMsg=" +
                                                    JMSTestConstants.STANDARD_DELAY_BETWEEN_MESSAGES + ",stopAfter=" +
                                                    JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, 0, 20);

        boolean sendSuccess =
                AndesClientUtils.getIfPublisherIsSuccess(sendingClient, JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT);

        assertTrue(receiveSuccess, "Message receiving failed");
        assertTrue(sendSuccess, "Message sending failed.");
    }

    /**
     * 1. Subscribe to a queue without selectors.
     * 2. Send messages without jms type as configured
     * 3. Send messages without jms type
     * 3. Verify that all the messages received by receiver
     */
    @Test(groups = "wso2.mb", description = "send-receive test case without selectors")
    public void performQueueSendWithoutSelectorsTestCase() {

        Integer messageCount = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
                                                      JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(),
                                                      ((Integer) (2 *
                                                                  JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT))
                                                              .toString(), "1",
                                                      "listener=true,ackMode=1,delayBetweenMsg=" +
                                                      JMSTestConstants.STANDARD_DELAY_BETWEEN_MESSAGES + ",stopAfter=" +
                                                      2 * JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT, "");

        receivingClient.startWorking();

        AndesClient sendingClient1 = new AndesClient("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
                                                     JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(),
                                                     JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT.toString(), "1",
                                                     "ackMode=1,delayBetweenMsg=" +
                                                     JMSTestConstants.STANDARD_DELAY_BETWEEN_MESSAGES + ",stopAfter=" +
                                                     JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT +
                                                     ",setJMSType=AAA", "");

        sendingClient1.startWorking();

        AndesClient sendingClient2 = new AndesClient("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
                                                     JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(),
                                                     JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT.toString(), "1",
                                                     "ackMode=1,delayBetweenMsg=" +
                                                     JMSTestConstants.STANDARD_DELAY_BETWEEN_MESSAGES + ",stopAfter=" +
                                                     JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT, "");

        sendingClient2.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, 2 * messageCount, 20);

        boolean sendSuccess1 = AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, messageCount);
        boolean sendSuccess2 = AndesClientUtils.getIfPublisherIsSuccess(sendingClient2, messageCount);

        //Sending messages with configured jmsType
        assertTrue(receiveSuccess, "Message receiving failed");
        //Sending message without jmsType
        assertTrue(sendSuccess1, "Message sending failed in Sender1");
        //receiving both sender's messages
        assertTrue(sendSuccess2, "Message sending failed in Sender2");
    }
}
