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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.engine.frameworkutils.FrameworkPathUtil;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import java.io.File;

/**
 * Checking Durable subscriber shared subscription ID option
 */
public class DurableTopicTestCase extends MBIntegrationBaseTest {

    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);

        super.serverManager = new ServerConfigurationManager(automationContext);

        // Replace the broker.xml with the allowSharedTopicSubscriptions configuration enabled under amqp
        // and restarts the server.
        super.serverManager.applyConfiguration(new File(FrameworkPathUtil.getSystemResourceLocation() + File.separator +
                        "artifacts" + File.separator + "mb" + File.separator + "config" + File.separator +
                        "allowSharedTopicSubscriptionsConfig" + File.separator + "broker.xml"),
                new File(ServerConfigurationManager.getCarbonHome() +
                        File.separator + "repository" + File.separator + "conf" + File.separator + "broker.xml"),
                true, true);


    }

    /**
     * 1. start a durable topic subscription
     * 2. send 1500 messages
     * 3. after 500 messages were received close the subscriber
     * 4. subscribe again. after 500 messages were received unsubscribe
     * 5. subscribe again. Verify no more messages are coming
     */
    @Test(groups = {"wso2.mb", "durableTopic"})
    public void performDurableTopicTestCase() {

        Integer sendCount = 1500;
        Integer runTime = 100;
        Integer expectedCount = 500;


        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:durableTopic1", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receivingSuccess1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount,
                runTime);


        //we just closed the subscription. Rest of messages should be delivered now.

        AndesClientUtils.sleepForInterval(2000);

        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0," +
                "unsubscribeAfter=" + expectedCount, "");

        receivingClient2.startWorking();


        boolean receivingSuccess2 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2, expectedCount,
                runTime);


        //now we have unsubscribed the topic subscriber no more messages should be received

        AndesClientUtils.sleepForInterval(2000);

        AndesClient receivingClient3 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0," +
                "unsubscribeAfter=" + expectedCount + ",stopAfter=" + expectedCount, "");
        receivingClient3.startWorking();

        boolean receivingSuccess3 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3, expectedCount,
                runTime);


        boolean sendingSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Assert.assertTrue(sendingSuccess, "Message sending failed.");

        Assert.assertTrue(receivingSuccess1, "Message receiving failed for client 1.");

        Assert.assertTrue(receivingSuccess2, "Message receiving failed for client 2.");

        Assert.assertFalse(receivingSuccess3, "Message received from client 3 when no more messages should be received.");

    }

    /**
     * Restore MB configurations after execute test
     *
     * @throws Exception
     */
    @AfterClass
    public void cleanUp() throws Exception {
        super.serverManager.restoreToLastConfiguration(true);
    }
}