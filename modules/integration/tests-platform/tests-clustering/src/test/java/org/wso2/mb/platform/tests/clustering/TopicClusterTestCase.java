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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.platform.tests.clustering;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.andes.stub.admin.types.Queue;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.carbon.event.stub.internal.xsd.TopicNode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.topic.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TopicClusterTestCase extends MBPlatformBaseTest {

    private static final Log log = LogFactory.getLog(TopicClusterTestCase.class);
    private AutomationContext automationContext1;
    private AutomationContext automationContext2;
    private TopicAdminClient topicAdminClient1;
    private TopicAdminClient topicAdminClient2;


    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {

        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);

        automationContext1 = getAutomationContextWithKey("mb002");
        automationContext2 = getAutomationContextWithKey("mb003");

        topicAdminClient1 = new TopicAdminClient(automationContext1.getContextUrls().getBackEndUrl(),
                super.login(automationContext1), ConfigurationContextProvider.getInstance().getConfigurationContext());

        topicAdminClient2 = new TopicAdminClient(automationContext2.getContextUrls().getBackEndUrl(),
                super.login(automationContext2), ConfigurationContextProvider.getInstance().getConfigurationContext());

    }

    @Test(groups = "wso2.mb", description = "Single topic Single node send-receive test case")
    public void testSingleTopicSingleNodeSendReceive() throws Exception {
        int sendCount = 1000;
        int expectedCount = 1000;

        String randomInstanceKey = getRandomMBInstance();

        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, "clusterSingleTopic1");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);



        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, "clusterSingleTopic1");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        TopicNode topic = topicAdminClient1.getTopicByName("clusterSingleTopic1");
        assertTrue(topic.getTopicName().equalsIgnoreCase("clusterSingleTopic1"), "Topic created in MB node 1 not exist");

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");


//        Integer sendCount = 1000;
//        Integer runTime = 20;
//        Integer expectedCount = 1000;
//
//        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
//                automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostinfo
//                , "topic:singleTopic1",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        boolean bQueueReplicated = false;
//        TopicNode topic = topicAdminClient1.getTopicByName("singleTopic1");
//
//        assertTrue(topic.getTopicName().equalsIgnoreCase("singleTopic1"), "Topic created in MB node 1 not exist");
//
//        AndesClient sendingClient = new AndesClient("send", hostinfo
//                , "topic:singleTopic1", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        assertEquals((receiveSuccess && sendSuccess), true);
    }


    @Test(groups = "wso2.mb", description = "Single topic replication")
    public void testSingleTopicReplication() throws Exception {

        String topic = "singleTopic2";
        boolean bQueueReplicated = false;

        topicAdminClient1.addTopic(topic);
        TopicNode topicNode = topicAdminClient2.getTopicByName(topic);

        assertTrue(topicNode != null && topicNode.getTopicName().equalsIgnoreCase(topic),
                "Topic created in MB node 1 not replicated in MB node 2");

        topicAdminClient2.removeTopic(topic);
        topicNode = topicAdminClient2.getTopicByName(topic);

        assertTrue(topicNode == null,
                "Topic deleted in MB node 2 not deleted in MB node 1");

    }

    @Test(groups = "wso2.mb", description = "Single topic Multi node send-receive test case")
    public void testSingleTopicMultiNodeSendReceive() throws Exception {
        int sendCount = 1000;
        int expectedCount = 1000;

        String randomInstanceKey = getRandomMBInstance();

        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, "clusterSingleTopic3");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext2.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext2.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, "clusterSingleTopic3");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        TopicNode topic = topicAdminClient1.getTopicByName("clusterSingleTopic1");

        assertTrue(topic.getTopicName().equalsIgnoreCase("clusterSingleTopic1"), "Topic created in MB node 1 not exist");

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");

//        Integer sendCount = 1000;
//        Integer runTime = 20;
//        Integer expectedCount = 1000;
//
//        String hostinfo1 = automationContext1.getInstance().getHosts().get("default") + ":" +
//                automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostinfo1
//                , "topic:singleTopic3",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        TopicNode topicNode = topicAdminClient2.getTopicByName("singleTopic3");
//
//        String hostinfo2 = automationContext2.getInstance().getHosts().get("default") + ":" +
//                automationContext2.getInstance().getPorts().get("amqp");
//
//        AndesClient sendingClient = new AndesClient("send", hostinfo2
//                , "topic:singleTopic3", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        assertEquals((receiveSuccess && sendSuccess), true);
    }


    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        topicAdminClient1.removeTopic("clusterSingleTopic1");
        topicAdminClient1.removeTopic("clusterSingleTopic2");
        topicAdminClient1.removeTopic("clusterSingleTopic3");
    }
}
