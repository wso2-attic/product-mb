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

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.carbon.event.stub.internal.xsd.TopicNode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.topic.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * This class includes tests subscribers/publishers with different rates for topics
 */
public class SingleSubscriberSinglePublisherTopicTestCase extends MBPlatformBaseTest {

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

    /**
     * Publish message to a topic in a single node and receive from the same node
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Same node publisher subscriber test case")
    public void testSameNodePubSub() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostinfo
                , "topic:singleTopic1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        TopicNode topic = topicAdminClient1.getTopicByName("singleTopic1");

        assertTrue(topic.getTopicName().equalsIgnoreCase("singleTopic1"), "Topic created in MB node 1 not exist");

        AndesClient sendingClient = new AndesClient("send", hostinfo
                , "topic:singleTopic1", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }

    /**
     * Publish message to a topic in a node and receive from the same node at a slow rate.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Same node publisher, slow subscriber test case")
    public void testSameNodeSlowSubscriber() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostinfo
                , "topic:singleTopic2",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=10,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        TopicNode topic = topicAdminClient1.getTopicByName("singleTopic2");

        assertTrue(topic.getTopicName().equalsIgnoreCase("singleTopic2"),
                "Topic created in MB node 1 not exist");

        AndesClient sendingClient = new AndesClient("send", hostinfo
                , "topic:singleTopic2", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }


    /**
     * Publish message at a slow rate to a topic in one node and and receive from the
     * same node.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Same node slow publisher test case")
    public void testSameNodeSlowPublisher() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostinfo
                , "topic:singleTopic3",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        TopicNode topic = topicAdminClient1.getTopicByName("singleTopic3");

        assertTrue(topic.getTopicName().equalsIgnoreCase("singleTopic3"),
                "Topic created in MB node 1 not exist");

        AndesClient sendingClient = new AndesClient("send", hostinfo
                , "topic:singleTopic3", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=10,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }

    /**
     * Publish message to a topic in a single node and receive from a different node
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Different node publisher subscriber test case")
    public void testDifferentNodePubSub() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostInfoForReceiver = automationContext1.getInstance().getHosts().get("default") +
                ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostInfoForReceiver
                , "topic:singleTopic4",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        TopicNode topic = topicAdminClient1.getTopicByName("singleTopic4");

        assertTrue(topic.getTopicName().equalsIgnoreCase("singleTopic4"),
                "Topic created in MB node 1 not exist");

        String hostInfoForSender = automationContext2.getInstance().getHosts().get("default") +
                ":" +
                automationContext2.getInstance().getPorts().get("amqp");
        AndesClient sendingClient = new AndesClient("send", hostInfoForSender
                , "topic:singleTopic4", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }

    /**
     * Publish message to a topic in a single node and receive from a different node at a slow
     * rate.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Different node slow subscriber test case")
    public void testDifferentNodeSlowSubscriber() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostInfoForReceiver = automationContext1.getInstance().getHosts().get("default") +
                ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostInfoForReceiver
                , "topic:singleTopic5",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=10,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        TopicNode topic = topicAdminClient1.getTopicByName("singleTopic5");

        assertTrue(topic.getTopicName().equalsIgnoreCase("singleTopic5"),
                "Topic created in MB node 1 not exist");

        String hostInfoForSender = automationContext2.getInstance().getHosts().get("default") +
                ":" +
                automationContext2.getInstance().getPorts().get("amqp");
        AndesClient sendingClient = new AndesClient("send", hostInfoForSender
                , "topic:singleTopic5", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }

    /**
     * Publish message to a topic in a single node and receive from a different node at a slow
     * rate.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Different node slow publisher test case")
    public void testDifferentNodeSlowPublisher() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostInfoForReceiver = automationContext1.getInstance().getHosts().get("default") +
                ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostInfoForReceiver
                , "topic:singleTopic6",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        TopicNode topic = topicAdminClient1.getTopicByName("singleTopic6");

        assertTrue(topic.getTopicName().equalsIgnoreCase("singleTopic6"),
                "Topic created in MB node 1 not exist");

        String hostInfoForSender = automationContext2.getInstance().getHosts().get("default") +
                ":" +
                automationContext2.getInstance().getPorts().get("amqp");
        AndesClient sendingClient = new AndesClient("send", hostInfoForSender
                , "topic:singleTopic6", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=10,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }

    /**
     * Publish message to a topic in a single node at a slower rate and receive from a different
     * node at a slow rate.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Different node slow publisher slow subscriber test " +
            "case")
    public void testDifferentNodeSlowPublisherSlowSubscriber() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostInfoForReceiver = automationContext1.getInstance().getHosts().get("default") +
                ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostInfoForReceiver
                , "topic:singleTopic7",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=10,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        TopicNode topic = topicAdminClient1.getTopicByName("singleTopic7");

        assertTrue(topic.getTopicName().equalsIgnoreCase("singleTopic7"),
                "Topic created in MB node 1 not exist");

        String hostInfoForSender = automationContext2.getInstance().getHosts().get("default") +
                ":" +
                automationContext2.getInstance().getPorts().get("amqp");
        AndesClient sendingClient = new AndesClient("send", hostInfoForSender
                , "topic:singleTopic7", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=10,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }


    /**
     * Publish message to a topic in a single node at a slower rate and receive from a different
     * node at a slow rate.
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Single node slow publisher slow subscriber test case")
    public void testSingleNodeSlowPublisherSlowSubscriber() throws Exception {
        // Max number of seconds to run the client
        Integer runTime = 80;
        // Expected message count
        Integer expectedCount = 2000;
        // Number of messages send
        Integer sendCount = 2000;

        String hostInfo = automationContext1.getInstance().getHosts().get("default") +
                ":" +
                automationContext1.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostInfo
                , "topic:singleTopic8",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=10,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        TopicNode topic = topicAdminClient1.getTopicByName("singleTopic8");

        assertTrue(topic.getTopicName().equalsIgnoreCase("singleTopic8"),
                "Topic created in MB node 1 not exist");

        AndesClient sendingClient = new AndesClient("send", hostInfo
                , "topic:singleTopic8", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=10,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }

    /**
     * Cleanup after running tests.
     *
     * @throws Exception
     */
    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        topicAdminClient1.removeTopic("singleTopic1");
        topicAdminClient1.removeTopic("singleTopic2");
        topicAdminClient1.removeTopic("singleTopic3");
        topicAdminClient1.removeTopic("singleTopic4");
        topicAdminClient1.removeTopic("singleTopic5");
        topicAdminClient1.removeTopic("singleTopic6");
        topicAdminClient1.removeTopic("singleTopic7");
        topicAdminClient1.removeTopic("singleTopic8");


    }
}
