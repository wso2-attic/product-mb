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
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;

import java.io.IOException;

import static org.testng.Assert.assertTrue;

/**
 * This class includes test cases with multiple topics.
 */
public class MultipleTopicTestCase extends MBPlatformBaseTest {

    private AutomationContext automationContext1;
    private TopicAdminClient topicAdminClient1;
    // Expected message count
    private static final long EXPECTED_COUNT = 2000L;
    // Number of messages send
    private static final long SEND_COUNT = 2000L;
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
    public void testMultipleTopicSingleNode()
            throws JMSException, AndesClientException, XPathExpressionException, NamingException,
                   IOException {
        

        AndesClient receivingClient1 = getAndesReceiverClient("topic1", EXPECTED_COUNT);
        AndesClient receivingClient2 = getAndesReceiverClient("topic2", EXPECTED_COUNT);
        AndesClient receivingClient3 = getAndesReceiverClient("topic3", EXPECTED_COUNT);
        AndesClient receivingClient4 = getAndesReceiverClient("topic4", EXPECTED_COUNT);
        AndesClient receivingClient5 = getAndesReceiverClient("topic5", EXPECTED_COUNT);
        AndesClient receivingClient6 = getAndesReceiverClient("topic6", EXPECTED_COUNT);
        AndesClient receivingClient7 = getAndesReceiverClient("topic7", EXPECTED_COUNT);
        AndesClient receivingClient8 = getAndesReceiverClient("topic8", EXPECTED_COUNT);
        AndesClient receivingClient9 = getAndesReceiverClient("topic9", EXPECTED_COUNT);
        AndesClient receivingClient10 = getAndesReceiverClient("topic10", EXPECTED_COUNT);

        receivingClient1.startClient();
        receivingClient2.startClient();
        receivingClient3.startClient();
        receivingClient4.startClient();
        receivingClient5.startClient();
        receivingClient6.startClient();
        receivingClient7.startClient();
        receivingClient8.startClient();
        receivingClient9.startClient();
        receivingClient10.startClient();

        AndesClient sendingClient1 = getAndesSenderClient("topic1", SEND_COUNT);
        AndesClient sendingClient2 = getAndesSenderClient("topic2", SEND_COUNT);
        AndesClient sendingClient3 = getAndesSenderClient("topic3", SEND_COUNT);
        AndesClient sendingClient4 = getAndesSenderClient("topic4", SEND_COUNT);
        AndesClient sendingClient5 = getAndesSenderClient("topic5", SEND_COUNT);
        AndesClient sendingClient6 = getAndesSenderClient("topic6", SEND_COUNT);
        AndesClient sendingClient7 = getAndesSenderClient("topic7", SEND_COUNT);
        AndesClient sendingClient8 = getAndesSenderClient("topic8", SEND_COUNT);
        AndesClient sendingClient9 = getAndesSenderClient("topic9", SEND_COUNT);
        AndesClient sendingClient10 = getAndesSenderClient("topic10", SEND_COUNT);

        sendingClient1.startClient();
        sendingClient2.startClient();
        sendingClient3.startClient();
        sendingClient4.startClient();
        sendingClient5.startClient();
        sendingClient6.startClient();
        sendingClient7.startClient();
        sendingClient8.startClient();
        sendingClient9.startClient();
        sendingClient10.startClient();




        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient2, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient3, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient4, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient5, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient6, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient7, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient8, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient9, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient10, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(sendingClient1.getSentMessageCount(), SEND_COUNT, "Messaging sending failed in sender 1");
        Assert.assertEquals(sendingClient2.getSentMessageCount(), SEND_COUNT, "Messaging sending failed in sender 2");
        Assert.assertEquals(sendingClient3.getSentMessageCount(), SEND_COUNT, "Messaging sending failed in sender 3");
        Assert.assertEquals(sendingClient4.getSentMessageCount(), SEND_COUNT, "Messaging sending failed in sender 4");
        Assert.assertEquals(sendingClient5.getSentMessageCount(), SEND_COUNT, "Messaging sending failed in sender 5");
        Assert.assertEquals(sendingClient6.getSentMessageCount(), SEND_COUNT, "Messaging sending failed in sender 6");
        Assert.assertEquals(sendingClient7.getSentMessageCount(), SEND_COUNT, "Messaging sending failed in sender 7");
        Assert.assertEquals(sendingClient8.getSentMessageCount(), SEND_COUNT, "Messaging sending failed in sender 8");
        Assert.assertEquals(sendingClient9.getSentMessageCount(), SEND_COUNT, "Messaging sending failed in sender 9");
        Assert.assertEquals(sendingClient10.getSentMessageCount(), SEND_COUNT, "Messaging sending failed in sender 10");
        
        
        Assert.assertEquals(receivingClient1.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive all the messages in receiving client 1");
        Assert.assertEquals(receivingClient2.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive all the messages in receiving client 2");
        Assert.assertEquals(receivingClient3.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive all the messages in receiving client 3");
        Assert.assertEquals(receivingClient4.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive all the messages in receiving client 4");
        Assert.assertEquals(receivingClient5.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive all the messages in receiving client 5");
        Assert.assertEquals(receivingClient6.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive all the messages in receiving client 6");
        Assert.assertEquals(receivingClient7.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive all the messages in receiving client 7");
        Assert.assertEquals(receivingClient8.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive all the messages in receiving client 8");
        Assert.assertEquals(receivingClient9.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive all the messages in receiving client 9");
        Assert.assertEquals(receivingClient10.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive all the messages in receiving client 10");

    }


    /**
     * Return AndesClient to subscriber for a given topic
     *
     * @param topicName       Name of the topic which the subscriber subscribes
     * @param expectedCount   Expected message count to be received
     * @return AndesClient object to receive messages
     */
    private AndesClient getAndesReceiverClient(String topicName, long expectedCount)
            throws NamingException, JMSException, AndesClientException, XPathExpressionException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, topicName);
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);


        return new AndesClient(consumerConfig);
        
        
        
        
//        // Max number of seconds to run the client
//        Integer runTime = 100;
//
//        return new AndesClient("receive", hostInformation
//                , "topic:" + topicName,
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
    }

    /**
     * Return AndesClient to send messages to a given topic
     *
     * @param topicName       Name of the topic
     * @param hostInformation IP address and port information
     * @param sendCount       Message count to be sent
     * @return AndesClient object to send messages
     */
    private AndesClient getAndesSenderClient(String topicName, long sendCount)
            throws XPathExpressionException, AndesClientException, NamingException, JMSException {


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, topicName);
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        return new AndesClient(publisherConfig);



//        // Max number of seconds to run the client
//        Integer runTime = 100;
//
//        return new AndesClient("send", hostInformation
//                , "topic:" + topicName, "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
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
