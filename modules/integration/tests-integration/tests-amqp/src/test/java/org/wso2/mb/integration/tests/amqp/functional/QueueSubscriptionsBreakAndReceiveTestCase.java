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

package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;


/**
 * 1. subscribe to a single queue which will take 1/5 messages of sent and stop
 * 2. send messages to the queue
 * 3. close and resubscribe 5 times to the queue
 * 4. verify message count is equal to the sent total
 */
public class QueueSubscriptionsBreakAndReceiveTestCase {

    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performQueueSubscriptionsBreakAndReceiveTestCase() {

        Integer sendCount = 1000;
        Integer runTime = 30;
        int numberOfSubscriptionBreaks = 5;
        Integer expectedCount = sendCount/numberOfSubscriptionBreaks;
        int totalMsgCountReceived = 0;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:breakSubscriberQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:breakSubscriberQueue", "100", "false",
                runTime.toString(),sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean success = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount , runTime);

        totalMsgCountReceived +=  receivingClient.getReceivedqueueMessagecount();

        //anyway wait one more iteration to verify no more messages are delivered
        for (int count = 1; count < numberOfSubscriptionBreaks ; count ++) {

            receivingClient.startWorking();
            AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount , runTime);
            totalMsgCountReceived +=  receivingClient.getReceivedqueueMessagecount();
            AndesClientUtils.sleepForInterval(1000);
        }

        if(totalMsgCountReceived == sendCount) {
            System.out.println("TEST PASSED");
        } else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals((totalMsgCountReceived == sendCount),true);
    }

}
