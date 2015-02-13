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

/**
 * This class includes test cases with multiple topics.
 */
public class MultipleTopicTestCase extends MBPlatformBaseTest {

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
     * Publish messages to a topic in a single node and receive from the same node
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Same node publisher subscriber test case")
    public void testMultipleTopicSingleNode() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 50;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient1 = getAndesReceiverClient("topic1", hostinfo, expectedCount);
        AndesClient receivingClient2 = getAndesReceiverClient("topic2", hostinfo, expectedCount);
        AndesClient receivingClient3 = getAndesReceiverClient("topic3", hostinfo, expectedCount);
        AndesClient receivingClient4 = getAndesReceiverClient("topic4", hostinfo, expectedCount);
        AndesClient receivingClient5 = getAndesReceiverClient("topic5", hostinfo, expectedCount);
        AndesClient receivingClient6 = getAndesReceiverClient("topic6", hostinfo, expectedCount);
        AndesClient receivingClient7 = getAndesReceiverClient("topic7", hostinfo, expectedCount);
        AndesClient receivingClient8 = getAndesReceiverClient("topic8", hostinfo, expectedCount);
        AndesClient receivingClient9 = getAndesReceiverClient("topic9", hostinfo, expectedCount);
        AndesClient receivingClient10 = getAndesReceiverClient("topic10", hostinfo, expectedCount);

        receivingClient1.startWorking();
        receivingClient2.startWorking();
        receivingClient3.startWorking();
        receivingClient4.startWorking();
        receivingClient5.startWorking();
        receivingClient6.startWorking();
        receivingClient7.startWorking();
        receivingClient8.startWorking();
        receivingClient9.startWorking();
        receivingClient10.startWorking();

        AndesClient sendingClient1 = getAndesSenderClient("topic1", hostinfo, sendCount);
        AndesClient sendingClient2 = getAndesSenderClient("topic2", hostinfo, sendCount);
        AndesClient sendingClient3 = getAndesSenderClient("topic3", hostinfo, sendCount);
        AndesClient sendingClient4 = getAndesSenderClient("topic4", hostinfo, sendCount);
        AndesClient sendingClient5 = getAndesSenderClient("topic5", hostinfo, sendCount);
        AndesClient sendingClient6 = getAndesSenderClient("topic6", hostinfo, sendCount);
        AndesClient sendingClient7 = getAndesSenderClient("topic7", hostinfo, sendCount);
        AndesClient sendingClient8 = getAndesSenderClient("topic8", hostinfo, sendCount);
        AndesClient sendingClient9 = getAndesSenderClient("topic9", hostinfo, sendCount);
        AndesClient sendingClient10 = getAndesSenderClient("topic10", hostinfo, sendCount);

        sendingClient1.startWorking();
        sendingClient2.startWorking();
        sendingClient3.startWorking();
        sendingClient4.startWorking();
        sendingClient5.startWorking();
        sendingClient6.startWorking();
        sendingClient7.startWorking();
        sendingClient8.startWorking();
        sendingClient9.startWorking();
        sendingClient10.startWorking();


        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, sendCount),
                "Messaging sending failed in sender 1");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient2, sendCount),
                "Messaging sending failed in sender 2");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient3, sendCount),
                "Messaging sending failed in sender 3");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient4, sendCount),
                "Messaging sending failed in sender 4");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient5, sendCount),
                "Messaging sending failed in sender 5");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient6, sendCount),
                "Messaging sending failed in sender 6");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient7, sendCount),
                "Messaging sending failed in sender 7");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient8, sendCount),
                "Messaging sending failed in sender 8");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient9, sendCount),
                "Messaging sending failed in sender 9");
        Assert.assertTrue(AndesClientUtils.getIfPublisherIsSuccess(sendingClient10, sendCount),
                "Messaging sending failed in sender 10");


        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1, expectedCount, runTime),
                "Did not receive all the messages in receiving client 1");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2, expectedCount, runTime),
                "Did not receive all the messages in receiving client 2");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3, expectedCount, runTime),
                "Did not receive all the messages in receiving client 3");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient4, expectedCount, runTime),
                "Did not receive all the messages in receiving client 4");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient5, expectedCount, runTime),
                "Did not receive all the messages in receiving client 5");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient6, expectedCount, runTime),
                "Did not receive all the messages in receiving client 6");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient7, expectedCount, runTime),
                "Did not receive all the messages in receiving client 7");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient8, expectedCount, runTime),
                "Did not receive all the messages in receiving client 8");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient9, expectedCount, runTime),
                "Did not receive all the messages in receiving client 9");
        Assert.assertTrue(AndesClientUtils.waitUntilMessagesAreReceived(receivingClient10, expectedCount, runTime),
                "Did not receive all the messages in receiving client 10");

    }


    /**
     * Return AndesClient to subscriber for a given topic
     *
     * @param topicName       Name of the topic which the subscriber subscribes
     * @param hostInformation IP address and port information
     * @param expectedCount   Expected message count to be received
     * @return AndesClient object to receive messages
     */
    private AndesClient getAndesReceiverClient(String topicName, String hostInformation, Integer expectedCount) {
        // Max number of seconds to run the client
        Integer runTime = 100;

        return new AndesClient("receive", hostInformation
                , "topic:" + topicName,
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
    }

    /**
     * Return AndesClient to send messages to a given topic
     *
     * @param topicName       Name of the topic
     * @param hostInformation IP address and port information
     * @param sendCount       Message count to be sent
     * @return AndesClient object to send messages
     */
    private AndesClient getAndesSenderClient(String topicName, String hostInformation,
                                             Integer sendCount) {
        // Max number of seconds to run the client
        Integer runTime = 100;

        return new AndesClient("send", hostInformation
                , "topic:" + topicName, "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
    }


    /**
     * Cleanup after running tests.
     *
     * @throws Exception
     */
    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        topicAdminClient1.removeTopic("topic1");
        topicAdminClient1.removeTopic("topic2");
        topicAdminClient1.removeTopic("topic3");
        topicAdminClient1.removeTopic("topic4");
        topicAdminClient1.removeTopic("topic5");
        topicAdminClient1.removeTopic("topic6");
        topicAdminClient1.removeTopic("topic7");
        topicAdminClient1.removeTopic("topic8");
        topicAdminClient1.removeTopic("topic9");
        topicAdminClient1.removeTopic("topic10");

    }

}
