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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.tests.JMSTestConstants;

/***
 * Unit tests to ensure jms expiration works as expected with durable topics.
 */
public class MixedDurableTopicTestCase {

    @BeforeClass
    public void prepare() {
        // Any initialisations / configurations should be made at this point.
        AndesClientUtils.sleepForInterval(15000);
    }


    /***
     * 1. Start durable topic subscriber
     * 2. Send 1000 messages with 400 messages having expiration
     * 3. Stop subscriber after receiving 300 messages
     * 4. Start subscriber
     * 5. Verify that the subscriber has received remaining 300 messages.
     * 6. Pass test case if and only if 600 messages in total have been received.
     */
    @Test(groups="wso2.mb", description = "Single durable topic send-receive test case with jms expiration")
    public void performDurableTopicTestCase() {

        //Calculate 50% of the message count without expiry
        Integer expectedMessageCountFromOneSubscriberSession = (JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants.SEND_MESSAGE_PERCENTAGE_WITHOUT_EXPIRY / 100))/2;
        Integer messageCountWithExpiration = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants.SEND_MESSAGE_PERCENTAGE_WITH_EXPIRY/2);
        Integer messageCountWithoutExpiration = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants.SEND_MESSAGE_PERCENTAGE_WITHOUT_EXPIRY/2);

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(), expectedMessageCountFromOneSubscriberSession.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0,stopAfter="+expectedMessageCountFromOneSubscriberSession.toString(), "");

        receivingClient.startWorking();

        AndesClient sendingClient1 = new AndesClient("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false"
                , JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(), messageCountWithoutExpiration.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+messageCountWithoutExpiration.toString(), "");

        sendingClient1.startWorking();

        AndesClient sendingClient2 = new AndesClient("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false",
                JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(), messageCountWithExpiration.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+messageCountWithExpiration +",jmsExpiration="+ JMSTestConstants.SAMPLE_JMS_EXPIRATION, "");

        sendingClient2.startWorking();

        boolean receivingSuccess1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedMessageCountFromOneSubscriberSession , JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS);

        boolean sendingSuccess1 = AndesClientUtils.getIfSenderIsSuccess(sendingClient1,messageCountWithoutExpiration);
        boolean sendingSuccess2 = AndesClientUtils.getIfSenderIsSuccess(sendingClient2,messageCountWithExpiration);

        //we just closed the subscription. Rest of messages should be delivered now.

        AndesClientUtils.sleepForInterval(2000);

        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(), expectedMessageCountFromOneSubscriberSession.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0,unsubscribeAfter="+expectedMessageCountFromOneSubscriberSession+",stopAfter="+expectedMessageCountFromOneSubscriberSession, "");
        receivingClient2.startWorking();

        boolean receivingSuccess2 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2, expectedMessageCountFromOneSubscriberSession , JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS);

        //now we have unsubscribed the topic subscriber no more messages should be received
        AndesClientUtils.sleepForInterval(2000);

        AndesClient receivingClient3 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(), expectedMessageCountFromOneSubscriberSession.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0,unsubscribeAfter="+expectedMessageCountFromOneSubscriberSession+",stopAfter="+expectedMessageCountFromOneSubscriberSession, "");
        receivingClient3.startWorking();

        boolean receivingSuccess3 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3, 0 , JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS);

        Assert.assertEquals((sendingSuccess1 && sendingSuccess2 && receivingSuccess1 && receivingSuccess2 && !receivingSuccess3), true);
    }
}
