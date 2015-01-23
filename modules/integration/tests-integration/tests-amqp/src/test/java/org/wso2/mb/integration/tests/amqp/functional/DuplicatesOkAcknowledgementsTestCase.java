/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

/**
 * This class includes test cases to test duplicate acknowledgements modes for queues
 */
public class DuplicatesOkAcknowledgementsTestCase extends MBIntegrationBaseTest {

    /**
     * Prepare environment for tests
     *
     * @throws Exception
     */
    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(1000);
    }

    /**
     * In this method we just test a sender and receiver with acknowledgements
     * 1. Start a queue receiver in client ack mode
     * 2. Receive messages acking message bunch to bunch
     * 3. Check whether all messages received
     */
    @Test(groups = "wso2.mb", description = "Single queue send-receive test case with dup messages")
    public void duplicatesOkAcknowledgementsTest() {
        Integer sendCount = 100;
        Integer runTime = 20;
        Integer expectedCount = 100;
        Integer duplicateCount;
        //Create receiving client
        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:dupOkAckTestQueue",
                                                      "100", "false", runTime.toString(), expectedCount.toString(),
                                                      "1", "listener=true,ackMode=3,delayBetweenMsg=10,stopAfter=500",
                                                      "");
        //Start receiving client
        receivingClient.startWorking();

        //Create sending client
        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:dupOkAckTestQueue", "100", "false",
                                                    runTime.toString(), sendCount.toString(), "1",
                                                    "ackMode=1,delayBetweenMsg=10,stopAfter=" + sendCount, "");
        //Start sending client
        sendingClient.startWorking();
        AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, 3 * expectedCount, runTime);
        Integer totalMessagesReceived = receivingClient.getReceivedqueueMessagecount();
        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);
        //Stop receiving client
        receivingClient.shutDownClient();
        //Get duplicates
        duplicateCount = receivingClient.getTotalNumberOfDuplicates();
        Assert.assertTrue(sendSuccess, "Messaging sending failed");
        Assert.assertEquals(totalMessagesReceived, (Integer) (expectedCount + duplicateCount),
                            "Total number of received message should be equal sum of expected and duplicate message count ");
    }
}
