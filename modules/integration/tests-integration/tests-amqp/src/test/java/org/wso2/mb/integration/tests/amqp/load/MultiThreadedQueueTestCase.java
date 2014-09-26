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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import static org.testng.Assert.assertEquals;

/**
 * send 10,000 messages collaberatively by 20 threads and receive collaberatively by 20 threads and
 * see if all messages are received
 */
public class MultiThreadedQueueTestCase {

    @BeforeClass
    public void init() throws Exception {
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = "wso2.mb",
          description = "Multiple queue senders - multiple queue receivers test case")
    public void performMultiThreadedQueueTestCase() {
        Integer sendCount = 10000;
        Integer numberOfPublisherThreads = 20;
        Integer numberOfSubscriberThreads = 20;
        Integer runTime = 200;
        Integer expectedCount = 10000;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672",
                                                      "queue:multiThreadQueue",
                                                      "100", "false", runTime.toString(),
                                                      expectedCount.toString(),
                                                      numberOfSubscriberThreads.toString(),
                                                      "listener=true,ackMode=1,delayBetweenMsg=0," +
                                                      "stopAfter=" + expectedCount,
                                                      "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672",
                                                    "queue:multiThreadQueue",
                                                    "100", "false",
                                                    runTime.toString(), sendCount.toString(),
                                                    numberOfPublisherThreads.toString(),
                                                    "ackMode=1,delayBetweenMsg=0," +
                                                    "stopAfter=" + sendCount,
                                                    "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils
                .waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        if (receiveSuccess && sendSuccess) {
            System.out.println("TEST PASSED");
        } else {
            System.out.println("TEST FAILED");
        }
        assertEquals((receiveSuccess && sendSuccess), true);
    }
}
