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

package org.wso2.mb.platform.tests.clustering.durable.topic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.topic.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

public class DurableTopicMessageDeliveringTestCase extends MBPlatformBaseTest{

    private static Log log = LogFactory.getLog(DurableTopicSubscriptionTestCase.class);

    private AutomationContext automationContext1;
    private AutomationContext automationContext2;
    private TopicAdminClient topicAdminClient1;

    /**
     * Prepare environment for tests.
     *
     * @throws Exception
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);

        automationContext1 = getAutomationContextWithKey("mb002");
        automationContext2 = getAutomationContextWithKey("mb003");
        topicAdminClient1 = new TopicAdminClient(automationContext1.getContextUrls().getBackEndUrl(),
                super.login(automationContext1), ConfigurationContextProvider.getInstance().getConfigurationContext());

    }

    @Test(groups = {"wso2.mb", "durableTopic"})
    public void performDurableTopicTestCase() throws Exception{

        Integer sendCount = 500;
        Integer runTime = 20;
        Integer expectedCount = 500;

        String hostInfoReceiver = automationContext1.getInstance().getHosts().get("default") +
                ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostInfoReceiver, "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=durableTopicSub1," +
                "delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        String hostInfoSender = automationContext1.getInstance().getHosts().get("default") +
                ":" +
                automationContext1.getInstance().getPorts().get("amqp");
        AndesClient sendingClient = new AndesClient("send", hostInfoSender, "topic:durableTopic", "100",
                "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receivingSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount,
                runTime);

        boolean sendingSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Assert.assertTrue(receivingSuccess, "Did not receive all the messages");
        Assert.assertTrue(sendingSuccess, "Messaging sending failed");

    }
}
