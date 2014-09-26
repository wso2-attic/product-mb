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
 * 1. define 15 queues
 * 2. send 30000 messages to 15 queues (2000 to each) by 45 threads.
 * 3. receive messages from 15 queues by 45 threads
 * 4. verify that all messages are received and no more messages are received
 */
public class MultiThreadedMultipleQueueTestCase {

    @BeforeClass
    public void prepare() {
        System.out.println(
                "=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performMultiThreadedMultipleQueueTestCase() {

        Integer sendCount = 30000;
        Integer runTime = 200;
        Integer numOfSendingThreads = 45;
        Integer numOfReceivingThreads = 45;
        int additional = 30;

        //wait some more time to see if more messages are received
        Integer expectedCount = sendCount + additional;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672",
                                                      "queue:Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15", "100",
                                                      "false",
                                                      runTime.toString(), expectedCount.toString(),
                                                      numOfReceivingThreads.toString(),
                                                      "listener=true,ackMode=1,delayBetweenMsg=0," +
                                                      "stopAfter=" + expectedCount,
                                                      "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672",
                                                    "queue:Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15", "100",
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
