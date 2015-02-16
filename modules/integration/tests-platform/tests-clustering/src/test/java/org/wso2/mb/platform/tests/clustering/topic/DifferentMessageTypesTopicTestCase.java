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
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.topic.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageType;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * This class includes test cases to test different types of messages (e.g. byte, map, object,
 * stream) which can be sent to a topic.
 */
public class DifferentMessageTypesTopicTestCase extends MBPlatformBaseTest {

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
     * Publish byte messages to a topic in a single node and receive from the same node with one
     * subscriber
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Single publisher single subscriber byte messages", enabled = true)
    public void testByteMessageSingleSubSinglePubTopic() throws Exception {

        this.runMessageTypeTestCase(JMSMessageType.BYTE, "byteTopic1");

//        // Max number of seconds to run the client
//        Integer runTime = 80;
//        // Expected message count
//        Integer expectedCount = 2000;
//        // Number of messages send
//        Integer sendCount = 2000;
//
//        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
//                          automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostinfo
//                , "topic:byteTopic1",
//                                                      "100", "false", runTime.toString(), expectedCount.toString(),
//                                                      "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClient sendingClient = new AndesClient("send", hostinfo
//                , "topic:byteTopic1", "100", "false",
//                                                    runTime.toString(), sendCount.toString(), "1",
//                                                    "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.setMessageType("byte");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived
//                (receivingClient, expectedCount, runTime);
//
//
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
//        Assert.assertTrue(sendSuccess, "Messaging sending failed");
    }

    /**
     * Publish map messages to a topic in a single node and receive from the same node with one
     * subscriber
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Single publisher single subscriber map messages",
            enabled = true)
    public void testMapMessageSingleSubSinglePubTopic() throws Exception {
        this.runMessageTypeTestCase(JMSMessageType.MAP, "mapTopic1");

//        // Max number of seconds to run the client
//        Integer runTime = 80;
//        // Expected message count
//        Integer expectedCount = 2000;
//        // Number of messages send
//        Integer sendCount = 2000;
//
//        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
//                          automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostinfo
//                , "topic:mapTopic1",
//                                                      "100", "false", runTime.toString(), expectedCount.toString(),
//                                                      "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClient sendingClient = new AndesClient("send", hostinfo
//                , "topic:mapTopic1", "100", "false",
//                                                    runTime.toString(), sendCount.toString(), "1",
//                                                    "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.setMessageType("map");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived
//                (receivingClient, expectedCount, runTime);
//
//
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
//        Assert.assertTrue(sendSuccess, "Messaging sending failed");
    }


    /**
     * Publish object messages to a topic in a single node and receive from the same node with one
     * subscriber
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Single publisher single subscriber object messages",
            enabled = true)
    public void testObjectMessageSingleSubSinglePubTopic() throws Exception {
        this.runMessageTypeTestCase(JMSMessageType.OBJECT, "objectTopic1");


//        // Max number of seconds to run the client
//        Integer runTime = 80;
//        // Expected message count
//        Integer expectedCount = 2000;
//        // Number of messages send
//        Integer sendCount = 2000;
//
//        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
//                          automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostinfo
//                , "topic:objectTopic1",
//                                                      "100", "false", runTime.toString(), expectedCount.toString(),
//                                                      "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClient sendingClient = new AndesClient("send", hostinfo
//                , "topic:objectTopic1", "100", "false",
//                                                    runTime.toString(), sendCount.toString(), "1",
//                                                    "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.setMessageType("object");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived
//                (receivingClient, expectedCount, runTime);
//
//
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
//        Assert.assertTrue(sendSuccess, "Messaging sending failed");
    }

    /**
     * Publish stream messages to a topic in a single node and receive from the same node with one
     * subscriber
     *
     * @throws Exception
     */
    @Test(groups = "wso2.mb", description = "Single publisher single subscriber stream messages",
            enabled = true)
    public void testStreamMessageSingleSubSinglePubTopic() throws Exception {
        this.runMessageTypeTestCase(JMSMessageType.STREAM, "streamTopic1");


//        // Max number of seconds to run the client
//        Integer runTime = 80;
//        // Expected message count
//        Integer expectedCount = 2000;
//        // Number of messages send
//        Integer sendCount = 2000;
//
//        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
//                          automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostinfo
//                , "topic:streamTopic1",
//                                                      "100", "false", runTime.toString(), expectedCount.toString(),
//                                                      "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClient sendingClient = new AndesClient("send", hostinfo
//                , "topic:streamTopic1", "100", "false",
//                                                    runTime.toString(), sendCount.toString(), "1",
//                                                    "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.setMessageType("stream");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived
//                (receivingClient, expectedCount, runTime);
//
//
//        boolean sendSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(receiveSuccess, "Did not receive all the messages");
//        Assert.assertTrue(sendSuccess, "Messaging sending failed");
    }

    /**
     * Cleanup after running tests.
     *
     * @throws Exception
     */
    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        topicAdminClient1.removeTopic("byteTopic1");
        topicAdminClient1.removeTopic("mapTopic1");
        topicAdminClient1.removeTopic("objectTopic1");
        topicAdminClient1.removeTopic("streamTopic1");
    }

    private void runMessageTypeTestCase(JMSMessageType messageType, String destinationName)
            throws XPathExpressionException, AndesClientException, NamingException, JMSException,
                   IOException {

        long expectedCount = 2000L;
        // Number of messages send
        long sendCount = 2000L;

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, destinationName);
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, destinationName);
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);
        publisherConfig.setJMSMessageType(messageType);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }
}
