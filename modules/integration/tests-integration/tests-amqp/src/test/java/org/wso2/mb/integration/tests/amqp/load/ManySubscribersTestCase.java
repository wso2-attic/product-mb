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

package org.wso2.mb.integration.tests.amqp.load;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import static org.testng.Assert.assertEquals;

/**
 * This class contains tests for receiving messages through a large number of subscribers.
 */
public class ManySubscribersTestCase extends MBIntegrationBaseTest {

    /**
     * Initialize the test as super tenant user.
     *
     * @throws Exception
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * Test message sending to 1000 subscribers at the same time.
     */
    @Test(groups = "wso2.mb", description = "Message content validation test case")
    public void performMillionMessageTestCase() {
        Integer sendCount = 100000;
        Integer runTime = 60 * 15; // 15 minutes
        Integer noOfSubscribers = 1000;
        Integer noOfPublishers = 1;

        Integer expectedCount = sendCount;
        String queueNameArg = "queue:ThousandSubscribers";

        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), expectedCount.toString(),
                noOfSubscribers.toString(), "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", queueNameArg, "100", "false",
                runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);

        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount();

        Assert.assertEquals(sendSuccess,"Message sending failed.");
        Assert.assertEquals(receiveSuccess,"Message receiving failed.");
        assertEquals(actualReceivedCount, sendCount, "Did not receive expected message count.");
    }
}
