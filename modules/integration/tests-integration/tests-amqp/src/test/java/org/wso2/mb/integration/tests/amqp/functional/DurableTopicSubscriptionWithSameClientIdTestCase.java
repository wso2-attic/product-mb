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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.engine.frameworkutils.FrameworkPathUtil;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import java.io.File;

import static org.testng.Assert.assertEquals;

public class DurableTopicSubscriptionWithSameClientIdTestCase extends MBIntegrationBaseTest {


    /**
     * Prepare environment for durable topic subscription with same client Id tests
     *
     * @throws Exception
     */
    @BeforeClass
    public void prepare() throws Exception {

        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);

        super.serverManager = new ServerConfigurationManager(automationContext);

        // Replace the broker.xml with the new configuration and restarts the server.
        super.serverManager.applyConfiguration(new File(FrameworkPathUtil.getSystemResourceLocation() + File.separator +
                        "artifacts" + File.separator + "mb" + File.separator + "config" + File.separator +
                        "allowSharedTopicSubscriptionsConfig" + File.separator + "broker.xml"),
                new File(ServerConfigurationManager.getCarbonHome() +
                        File.separator + "repository" + File.separator + "conf" + File.separator + "broker.xml"),
                true, true);

    }


    /**
     * Start 3 durable subscribers. Start publisher which sends 12 messages.
     * Get the total count received by all durable subscribers and compare with sent message count of the publisher.
     *
     */
    @Test(groups = {"wso2.mb", "durableTopic"})
    public void performDurableTopicWithSameClientIdTestCase() {

        Integer sendCount = 12;
        Integer runTime = 20;
        Integer expectedCount = 500;

        // Start subscription 1
        AndesClient receivingClient1 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");
        receivingClient1.startWorking();

        // Start subscription 2
        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");
        receivingClient2.startWorking();

        // Start subscription 3
        AndesClient receivingClient3 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");
        receivingClient3.startWorking();

        // Start message publisher
        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
        sendingClient.startWorking();

        AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1, expectedCount, runTime);
        AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2, expectedCount, runTime);
        AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3, expectedCount, runTime);

        int sendCountInt = (Integer) sendCount;

        int receivingCountClient1 = receivingClient1.getReceivedTopicMessagecount();
        int receivingCountClient2 = receivingClient2.getReceivedTopicMessagecount();
        int receivingCountClient3 = receivingClient3.getReceivedTopicMessagecount();

        int totalReceivingMessageCount = receivingCountClient1 + receivingCountClient2 + receivingCountClient3;

        assertEquals(sendCountInt,totalReceivingMessageCount,
                "Message receive count not equal to sent message count.");


    }

}
