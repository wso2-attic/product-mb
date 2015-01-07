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
 * This class includes test cases to test transacted acknowledgements modes for queues
 */
public class TransactedAcknowledgementsTestCase extends MBIntegrationBaseTest {

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
     * 1. Start a queue receiver with transacted sessions
     * 2. Send 10 messages
     * 3. After 10 messages are received rollback session
     * 4. After 50 messages received commit the session and close subscriber
     * 5. Analyse and see if each message is duplicated five times
     */
    @Test(groups = "wso2.mb", description = "Single queue send-receive test case with transactions")
    public void transactedAcknowledgements() {
        Integer sendCount = 10;
        Integer runTime = 20;
        int expectedCount = 10;
        //Create receiving client
        AndesClient receivingClient =
                new AndesClient("receive", "127.0.0.1:5672", "queue:transactedAckTestQueue", "100", "true",
                                runTime.toString(), String.valueOf(expectedCount), "1",
                                "listener=true,ackMode=0,delayBetweenMsg=100,stopAfter=100,rollbackAfterEach=10,commitAfterEach=30",
                                "");
        //Start receiving client
        receivingClient.startWorking();
        //Create sending client
        AndesClient sendingClient =
                new AndesClient("send", "127.0.0.1:5672", "queue:transactedAckTestQueue", "100", "false",
                                runTime.toString(), sendCount.toString(), "1",
                                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
        //Start sending client
        sendingClient.startWorking();
        AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
        int totalMessagesReceived = receivingClient.getReceivedqueueMessagecount();
        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);
        //If received messages less than expected number wait until received again
        //Get rollback status , check message id of next message of roll backed message equal to first message
        int duplicateCount = receivingClient.getTotalNumberOfDuplicates();
        Assert.assertTrue(sendSuccess, "Messaging sending failed");
        Assert.assertEquals(totalMessagesReceived, (expectedCount + duplicateCount),
                            "Total number of received message should be equal sum of expected and duplicate message count ");
        Assert.assertTrue(receivingClient.transactedOperation(10),
                          "After rollback next message need to equal first message of batch");
    }
}
