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
import org.wso2.carbon.event.stub.internal.TopicManagerAdminServiceEventAdminExceptionException;
import org.wso2.carbon.event.stub.internal.xsd.TopicNode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.topic.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * This class includes tests with multiple subscribers for a topic
 */
public class MultipleSubscriberMultiplePublisherTopicTestCase extends MBPlatformBaseTest {

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
     * Publish message to a single topic in a single node by one publisher and subscribe to
     * that topic with two subscribers
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Single node single publisher two subscribers test " +
            "case" , enabled = true)
    public void testMultipleSubscribers()
            throws AndesClientException, XPathExpressionException, NamingException, JMSException,
                   IOException, TopicManagerAdminServiceEventAdminExceptionException {
        long sendCount = 2000L;
        long expectedCount = 2000L;


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration initialConsumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, "mulSubTopic1");
        // Amount of message to receive
        initialConsumerConfig.setMaximumMessagesToReceived(expectedCount);
        initialConsumerConfig.setPrintsPerMessageCount(expectedCount / 10L);


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration secondaryConsumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, "mulSubTopic1");
        // Amount of message to receive
        secondaryConsumerConfig.setMaximumMessagesToReceived(expectedCount);
        secondaryConsumerConfig.setPrintsPerMessageCount(expectedCount / 10L);



        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, "mulSubTopic1");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient initialConsumerClient = new AndesClient(initialConsumerConfig);
        initialConsumerClient.startClient();

        AndesClient secondaryConsumerClient = new AndesClient(secondaryConsumerConfig);
        secondaryConsumerClient.startClient();

        TopicNode topic = topicAdminClient1.getTopicByName("mulSubTopic1");
        assertTrue(topic.getTopicName().equalsIgnoreCase("mulSubTopic1"), "Topic created in MB node 1 not exist");

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(initialConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(secondaryConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(initialConsumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
        Assert.assertEquals(secondaryConsumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");




//        // Max number of seconds to run the client
//        Integer runTime = 80;
//        // Expected message count
//        Integer expectedCount = 2000;
//        // Number of messages send
//        Integer sendCount = 2000;
//
//        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
//                automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient1 = new AndesClient("receive", hostinfo
//                , "topic:mulSubTopic1",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient1.startWorking();
//
//        AndesClient receivingClient2 = new AndesClient
//                ("receive", hostinfo
//                , "topic:mulSubTopic1",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient2.startWorking();
//
//        TopicNode topic = topicAdminClient1.getTopicByName("mulSubTopic1");
//
//        assertTrue(topic.getTopicName().equalsIgnoreCase("mulSubTopic1"), "Topic created in MB node 1 not exist");
//
//        AndesClient sendingClient = new AndesClient("send", hostinfo
//                , "topic:mulSubTopic1", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccessInClient1 = AndesClientUtils.waitUntilMessagesAreReceived
//                (receivingClient1, expectedCount, runTime);
//        boolean receiveSuccessInClient2 = AndesClientUtils.waitUntilMessagesAreReceived
//                (receivingClient1, expectedCount, runTime);
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(receiveSuccessInClient1, "Did not receive all the messages by the " +
//                "receiving client 1");
//        Assert.assertTrue(receiveSuccessInClient2, "Did not receive all the messages by the " +
//                "receiving client 2");
//        Assert.assertTrue(sendSuccess,"Message sending failed");
    }


    /**
     * Publish message to a single topic in a single node by one publisher and subscribe to
     * that topic with many subscribers
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Single node single publisher multiple subscribers " +
            "test case", enabled = true)
    public void testBulkSubscribers() throws Exception {
        long sendCount = 2000L;
        long expectedCount = 100000;


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                            Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                            ExchangeType.TOPIC, "mulSubTopic2");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, "mulSubTopic2");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig, 50);
        consumerClient.startClient();

        TopicNode topic = topicAdminClient1.getTopicByName("mulSubTopic2");
        assertTrue(topic.getTopicName().equalsIgnoreCase("mulSubTopic2"), "Topic created in MB node 1 not exist");

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");



//        // Max number of seconds to run the client
//        Integer runTime = 80;
//        // Expected message count
//        Integer expectedCount = 100000;
//        // Number of messages send
//        Integer sendCount = 2000;
//
//        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
//                automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostinfo
//                , "topic:mulSubTopic2",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "50", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        TopicNode topic = topicAdminClient1.getTopicByName("mulSubTopic2");
//
//        assertTrue(topic.getTopicName().equalsIgnoreCase("mulSubTopic2"), "Topic created in MB node 1 not exist");
//
//        AndesClient sendingClient = new AndesClient("send", hostinfo
//                , "topic:mulSubTopic2", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess= AndesClientUtils.waitUntilMessagesAreReceived
//                (receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
//        Assert.assertTrue(sendSuccess,"Message sending failed");
    }


    /**
     * Publish message to a single topic in a single node by multiple publishers and subscribe to
     * that topic with one subscribers
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Single node multiple publishers single subscriber " +
            "test case", enabled = true)
    public void testBulkPublishers() throws Exception {
        long sendCount = 100000L;
        long expectedCount = 100000L;


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, "mulSubTopic3");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, "mulSubTopic3");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        TopicNode topic = topicAdminClient1.getTopicByName("mulSubTopic2");
        assertTrue(topic.getTopicName().equalsIgnoreCase("mulSubTopic2"), "Topic created in MB node 1 not exist");

        AndesClient publisherClient = new AndesClient(publisherConfig, 50);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");




//        // Max number of seconds to run the client
//        Integer runTime = 200;
//        // Expected message count
//        Integer expectedCount = 100000;
//        // Number of messages send
//        Integer sendCount = 100000;
//
//        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
//                automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostinfo
//                , "topic:mulSubTopic3",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        TopicNode topic = topicAdminClient1.getTopicByName("mulSubTopic3");
//
//        assertTrue(topic.getTopicName().equalsIgnoreCase("mulSubTopic3"), "Topic created in MB node 1 not exist");
//
//        AndesClient sendingClient = new AndesClient("send", hostinfo
//                , "topic:mulSubTopic3", "100", "false",
//                runTime.toString(), sendCount.toString(), "50",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess= AndesClientUtils.waitUntilMessagesAreReceived
//                (receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
//        Assert.assertTrue(sendSuccess,"Message sending failed");
    }

    /**
     * Publish message to a single topic in a single node by multiple publishers and subscribe to
     * that topic with multiple subscribers
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Single node multiple publishers multiple " +
            "subscribers test case", enabled = true)
    public void testBulkPublishersBulkSubscribers() throws Exception {
        long sendCount = 2000L;
        long expectedCount = 100000L;


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, "mulSubTopic4");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, "mulSubTopic4");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig, 50);
        consumerClient.startClient();

        TopicNode topic = topicAdminClient1.getTopicByName("mulSubTopic4");
        assertTrue(topic.getTopicName().equalsIgnoreCase("mulSubTopic4"), "Topic created in MB node 1 not exist");

        AndesClient publisherClient = new AndesClient(publisherConfig, 50);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");




//        // Max number of seconds to run the client
//        Integer runTime = 200;
//        // Expected message count
//        Integer expectedCount = 100000;
//        // Number of messages send
//        Integer sendCount = 2000;
//
//        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
//                automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostinfo
//                , "topic:mulSubTopic4",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "50", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        TopicNode topic = topicAdminClient1.getTopicByName("mulSubTopic4");
//
//        assertTrue(topic.getTopicName().equalsIgnoreCase("mulSubTopic4"),
//                "Topic created in MB node 1 not exist");
//
//        AndesClient sendingClient = new AndesClient("send", hostinfo
//                , "topic:mulSubTopic4", "100", "false",
//                runTime.toString(), sendCount.toString(), "50",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess= AndesClientUtils.waitUntilMessagesAreReceived
//                (receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
//        Assert.assertTrue(sendSuccess,"Message sending failed");
    }


    /**
     * Publish message to a single topic in a single node by multiple publishers and subscribe to
     * that topic with multiple subscribers from another node
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "multiple node multiple publishers multiple " +
            "subscribers test case", enabled = true)
    public void testBulkPublishersBulkSubscribersDifferentNodes() throws Exception {
        long sendCount = 2000L;
        long expectedCount = 100000L;


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, "mulSubTopic5");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext2.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext2.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, "mulSubTopic5");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig, 50);
        consumerClient.startClient();

        TopicNode topic = topicAdminClient1.getTopicByName("mulSubTopic5");
        assertTrue(topic.getTopicName().equalsIgnoreCase("mulSubTopic5"), "Topic created in MB node 1 not exist");

        AndesClient publisherClient = new AndesClient(publisherConfig, 50);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");





//        // Max number of seconds to run the client
//        Integer runTime = 80;
//        // Expected message count
//        Integer expectedCount = 50000;
//        // Number of messages send
//        Integer sendCount = 1000;
//
//        String hostInfoReceiver = automationContext1.getInstance().getHosts().get("default") +
//                ":" +
//                automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostInfoReceiver
//                , "topic:mulSubTopic5",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "50", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        TopicNode topic = topicAdminClient1.getTopicByName("mulSubTopic5");
//
//        assertTrue(topic.getTopicName().equalsIgnoreCase("mulSubTopic5"),
//                "Topic created in MB node 1 not exist");
//
//        String hostInfoSender = automationContext1.getInstance().getHosts().get("default") +
//                ":" +
//                automationContext2.getInstance().getPorts().get("amqp");
//
//        AndesClient sendingClient = new AndesClient("send", hostInfoSender
//                , "topic:mulSubTopic5", "100", "false",
//                runTime.toString(), sendCount.toString(), "50",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess= AndesClientUtils.waitUntilMessagesAreReceived
//                (receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
//        Assert.assertTrue(sendSuccess,"Message sending failed");
    }



    /**
     * Cleanup after running tests.
     *
     * @throws Exception
     */
    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        topicAdminClient1.removeTopic("mulSubTopic1");
        topicAdminClient1.removeTopic("mulSubTopic2");
        topicAdminClient1.removeTopic("mulSubTopic3");
        topicAdminClient1.removeTopic("mulSubTopic4");
        topicAdminClient1.removeTopic("mulSubTopic5");


    }

}
