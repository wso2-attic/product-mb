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
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

/**
 * 1. start a queue receiver in client ack mode
 * 2. receive messages acking message bunch to bunch
 * 3. after all messages are received subscribe again and verify n more messages come
 */
public class ClientAcknowledgementsTestCase extends MBIntegrationBaseTest {

    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performClientAcknowledgementsTestCase() {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;
        Integer totalMsgsReceived = 0;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:clientAckTestQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=2,delayBetweenMsg=0,ackAfterEach=200,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:clientAckTestQueue", "100",
                "false",
                runTime.toString(), sendCount.toString(), "1", "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount,
                "");

        sendingClient.startWorking();

        boolean success = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        totalMsgsReceived += receivingClient.getReceivedqueueMessagecount();

        AndesClientUtils.sleepForInterval(2000);

        receivingClient.startWorking();
        AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, 15);

        totalMsgsReceived += receivingClient.getReceivedqueueMessagecount();

        Assert.assertEquals(expectedCount, totalMsgsReceived);
    }

}
