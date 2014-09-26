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

public class MultiThreadedMultipleQueueTopicTestCase {

    @BeforeClass
    public void prepare() {
        System.out.println(
                "=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb"})
    public void performMultiThreadedMultipleQueueTopicTestCase() {

        Integer queueSendCount = 30000;
        Integer queueRunTime = 200;
        Integer queueNumOfSendingThreads = 45;
        Integer queueNumOfReceivingThreads = 45;
        int additional = 30;

        Integer topicSendCount = 30000;
        Integer topicRunTime = 200;
        Integer topicNumOfSendingThreads = 15;
        Integer topicNumOfReceivingThreads = 45;

        //wait some more time to see if more messages are received
        Integer queueExpectedCount = 3*2000*15 + additional;

        //wait some more time to see if more messages are received
        Integer topicExpectedCount = topicSendCount + additional;

        AndesClient queueReceivingClient = new AndesClient("receive", "127.0.0.1:5672",
                                                      "queue:Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15", "100",
                                                      "false",
                                                      queueRunTime.toString(), queueExpectedCount.toString(),
                                                      queueNumOfReceivingThreads.toString(),
                                                      "listener=true,ackMode=1,delayBetweenMsg=0," +
                                                      "stopAfter=" + queueExpectedCount,
                                                      "");

        queueReceivingClient.startWorking();


        AndesClient topicReceivingClient = new AndesClient("receive", "127.0.0.1:5672",
                                                      "topic:T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15", "100",
                                                      "false",
                                                      topicRunTime.toString(), topicExpectedCount.toString(),
                                                      topicNumOfReceivingThreads.toString(),
                                                      "listener=true,ackMode=1,delayBetweenMsg=0," +
                                                      "stopAfter=" + topicExpectedCount,
                                                      "");

        topicReceivingClient.startWorking();


        AndesClient queueSendingClient = new AndesClient("send", "127.0.0.1:5672",
                                                    "queue:Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15", "100",
                                                    "false", queueRunTime.toString(),
                                                    queueSendCount.toString(), queueNumOfSendingThreads.toString(),
                                                    "ackMode=1,delayBetweenMsg=0," +
                                                    "stopAfter=" + queueSendCount,
                                                    "");

        queueSendingClient.startWorking();


        AndesClient topicSendingClient = new AndesClient("send", "127.0.0.1:5672",
                                                    "topic:T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15", "100",
                                                    "false", topicRunTime.toString(),
                                                    topicSendCount.toString(), topicNumOfSendingThreads.toString(),
                                                    "ackMode=1,delayBetweenMsg=0," +
                                                    "stopAfter=" + topicSendCount,
                                                    "");

        topicSendingClient.startWorking();

        //let us wait topic message receive time which is larger
        boolean success = AndesClientUtils
                .waitUntilMessagesAreReceived(topicReceivingClient, topicExpectedCount, topicRunTime);

        boolean queueReceiveSuccess = false;
        boolean topicReceiveSuccess = false;

        if ((queueExpectedCount - additional) == queueReceivingClient.getReceivedqueueMessagecount()) {
            queueReceiveSuccess = true;
        }

        if ((topicExpectedCount - additional) == topicReceivingClient.getReceivedqueueMessagecount()) {
            topicReceiveSuccess = true;
        }

        if (queueReceiveSuccess && topicReceiveSuccess) {
            System.out.println("TEST PASSED");
        } else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals((queueReceiveSuccess && topicReceiveSuccess), true);
    }
}
