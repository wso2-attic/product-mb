/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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
package org.wso2.mb.integration.tests.jms.expiration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;
import org.wso2.mb.integration.tests.JMSTestConstants;

import static org.testng.Assert.assertEquals;

/***
 * This class includes unit tests to verify that messages with JMS expiration are properly removed when delivering to queues.
 */
public class MixedQueueTestCase extends MBIntegrationBaseTest {

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    /***
     * 1. Subscribe to a queue.
     * 2. Send messages with and without expiry as configured
     * 3. Verify that only messages without expiry have been received and that both types of messages have been sent.
     */
    @Test(groups = "wso2.mb", description = "Single queue send-receive test case with 50% expired messages")
    public void performSingleQueueSendReceiveTestCase() {

        Integer expectedMessageCountFromSubscriberSession = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants.SEND_MESSAGE_PERCENTAGE_WITHOUT_EXPIRY / 100);
        Integer messageCountWithExpiration = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants.SEND_MESSAGE_PERCENTAGE_WITH_EXPIRY/2);
        Integer messageCountWithoutExpiration = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants.SEND_MESSAGE_PERCENTAGE_WITHOUT_EXPIRY/2);

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:singleQueue",
                "100", "false", JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(), expectedMessageCountFromSubscriberSession.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg="+ JMSTestConstants.STANDARD_DELAY_BETWEEN_MESSAGES+",stopAfter="+expectedMessageCountFromSubscriberSession, "");

        receivingClient.startWorking();

        AndesClient sendingClient1 = new AndesClient("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
                JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(), messageCountWithoutExpiration.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+messageCountWithoutExpiration, "");

        sendingClient1.startWorking();

        AndesClient sendingClient2 = new AndesClient("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
                JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(), messageCountWithExpiration.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+messageCountWithExpiration+",jmsExpiration="+ JMSTestConstants.SAMPLE_JMS_EXPIRATION, "");

        sendingClient2.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedMessageCountFromSubscriberSession, JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS);

        boolean sendSuccess1 = AndesClientUtils.getIfSenderIsSuccess(sendingClient1, JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS);
        boolean sendSuccess2 = AndesClientUtils.getIfSenderIsSuccess(sendingClient2, JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS);

        assertEquals((receiveSuccess && sendSuccess1 && sendSuccess2), true);
    }

    /***
     *  1. Start two subscribers
     *  2. Send messages with and without expiration as configured
     *  3. Verify that the total number of messages received by both subscribers is equal to message count sent with no expiration.
     */
    @Test(groups = "wso2.mb", description = "send messages to a queue which has two consumers with jms expiration")
    public void performManyConsumersTestCase() {

        Integer expectedMessageCountFromOneSubscriberSession = (JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants.SEND_MESSAGE_PERCENTAGE_WITHOUT_EXPIRY / 100)) /2;
        Integer messageCountWithExpiration = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants.SEND_MESSAGE_PERCENTAGE_WITH_EXPIRY/2);
        Integer messageCountWithoutExpiration = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants.SEND_MESSAGE_PERCENTAGE_WITHOUT_EXPIRY/2);

        AndesClient receivingClient1 = new AndesClient("receive", "127.0.0.1:5672", "queue:queue1",
                "100", "false", JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(), expectedMessageCountFromOneSubscriberSession.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedMessageCountFromOneSubscriberSession, "");

        receivingClient1.startWorking();

        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672", "queue:queue1",
                "100", "false", JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(), expectedMessageCountFromOneSubscriberSession.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedMessageCountFromOneSubscriberSession, "");
        receivingClient2.startWorking();

        AndesClient sendingClient1 = new AndesClient("send", "127.0.0.1:5672", "queue:queue1", "100", "false",
                JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(), messageCountWithoutExpiration.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + messageCountWithoutExpiration, "");

        sendingClient1.startWorking();

        AndesClient sendingClient2 = new AndesClient("send", "127.0.0.1:5672", "queue:queue1", "100", "false",
                JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(), messageCountWithExpiration.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + messageCountWithExpiration+",jmsExpiration="+ JMSTestConstants.SAMPLE_JMS_EXPIRATION, "");

        sendingClient2.startWorking();

        int msgCountFromClient1 = AndesClientUtils.getNoOfMessagesReceived(receivingClient1, expectedMessageCountFromOneSubscriberSession, JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS);
        int msgCountFromClient2 = AndesClientUtils.getNoOfMessagesReceived(receivingClient2, expectedMessageCountFromOneSubscriberSession, JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS);

        boolean sendSuccess1 = AndesClientUtils.getIfSenderIsSuccess(sendingClient1, JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS);
        boolean sendSuccess2 = AndesClientUtils.getIfSenderIsSuccess(sendingClient2, JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS);

        assertEquals((msgCountFromClient1+msgCountFromClient2 == messageCountWithExpiration) && sendSuccess1 && sendSuccess2,true);
    }

}
