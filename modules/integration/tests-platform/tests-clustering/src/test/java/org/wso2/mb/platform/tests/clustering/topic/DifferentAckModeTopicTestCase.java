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

package org.wso2.mb.platform.tests.clustering.topic;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.topic.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import static org.testng.Assert.assertEquals;

/**
 * This class includes test cases to test different ack modes for topics
 */
public class DifferentAckModeTopicTestCase extends MBPlatformBaseTest {

    private AutomationContext automationContext1;
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

        topicAdminClient1 = new TopicAdminClient(automationContext1.getContextUrls().getBackEndUrl(),
                super.login(automationContext1), ConfigurationContextProvider.getInstance().getConfigurationContext());

    }


    /**
     * Publish messages to a topic in single node and receive from the same node with
     * SESSION_TRANSACTED ack mode
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "SESSION_TRANSACTED ack mode test case for topic", enabled = true)
    public void testSessionTransactedAckModeForTopic() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostinfo
                , "topic:sessionTransactedAckTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=0,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", hostinfo
                , "topic:sessionTransactedAckTopic", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=0,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived
                (receivingClient, expectedCount, runTime);


        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
        Assert.assertTrue(sendSuccess, "Messaging sending failed");
    }

    /**
     * Publish messages to a topic in single node and receive from the same node with
     * AUTO_ACKNOWLEDGE ack mode
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "AUTO_ACKNOWLEDGE ack mode test case for topic", enabled = true)
    public void testAutoAcknowledgeModeForTopic() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostinfo
                , "topic:autoAcknowledgeTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", hostinfo
                , "topic:autoAcknowledgeTopic", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived
                (receivingClient, expectedCount, runTime);


        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
        Assert.assertTrue(sendSuccess, "Messaging sending failed");
    }

    /**
     * Publish messages to a topic in single node and receive from the same node with
     * CLIENT_ACKNOWLEDGE ack mode
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "CLIENT_ACKNOWLEDGE ack mode test case for topic", enabled = true)
    public void testClientAcknowledgeModeForTopic() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostinfo
                , "topic:clientAcknowledgeTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=2,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", hostinfo
                , "topic:clientAcknowledgeTopic", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=2,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived
                (receivingClient, expectedCount, runTime);


        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
        Assert.assertTrue(sendSuccess, "Messaging sending failed");
    }

    /**
     * Publish messages to a topic in single node and receive from the same node with
     * DUPS_OK_ACKNOWLEDGE ack mode
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "DUPS_OK_ACKNOWLEDGE ack mode test case for topic", enabled = true)
    public void testDupOkAcknowledgeModeForTopic() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostinfo
                , "topic:dupsOkAcknowledgeTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=3,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", hostinfo
                , "topic:dupsOkAcknowledgeTopic", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=3,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived
                (receivingClient, expectedCount, runTime);


        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
        Assert.assertTrue(sendSuccess, "Messaging sending failed");
    }

    /**
     * Cleanup after running tests.
     *
     * @throws Exception
     */
    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        topicAdminClient1.removeTopic("sessionTransactedAckTopic");
        topicAdminClient1.removeTopic("autoAcknowledgeTopic");
        topicAdminClient1.removeTopic("clientAcknowledgeTopic");
        topicAdminClient1.removeTopic("dupsOkAcknowledgeTopic");


    }

}
