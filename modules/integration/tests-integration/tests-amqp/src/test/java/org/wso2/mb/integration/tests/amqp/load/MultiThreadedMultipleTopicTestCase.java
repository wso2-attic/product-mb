/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.mb.integration.tests.amqp.load;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

/**
 * 1. Create 45 topic subscribers (there will be three for each topic) thus there will be 15 topics
 * 2. Send 30000 messages , 2000 for each topic
 * 3. verify that all messages are received and no more messages are received
 */
public class MultiThreadedMultipleTopicTestCase {

    @BeforeClass
    public void prepare() {
        System.out.println(
                "=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "topic"})
    public void performMultiThreadedMultipleTopicTestCase() {

        Integer sendCount = 30000;
        Integer runTime = 200;
        Integer numOfSendingThreads = 15;
        Integer numOfReceivingThreads = 45;
        int additional = 30;

        //wait some more time to see if more messages are received
        Integer expectedCount = 3*2000*15 + additional;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672",
                                                      "topic:T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15", "100",
                                                      "false",
                                                      runTime.toString(), expectedCount.toString(),
                                                      numOfReceivingThreads.toString(),
                                                      "listener=true,ackMode=1,delayBetweenMsg=0," +
                                                      "stopAfter=" + expectedCount,
                                                      "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672",
                                                    "topic:T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15", "100",
                                                    "false", runTime.toString(),
                                                    sendCount.toString(), numOfSendingThreads.toString(),
                                                    "ackMode=1,delayBetweenMsg=0," +
                                                    "stopAfter=" + sendCount,
                                                    "");

        sendingClient.startWorking();

        boolean success = AndesClientUtils
                .waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean receiveSuccess = false;
        if ((expectedCount - additional) == receivingClient.getReceivedqueueMessagecount()) {
            receiveSuccess = true;
        }

        if (receiveSuccess) {
            System.out.println("TEST PASSED");
        } else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals(receiveSuccess, true);
    }
}
