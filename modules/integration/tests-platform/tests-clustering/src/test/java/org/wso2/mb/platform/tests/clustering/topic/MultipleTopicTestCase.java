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
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.carbon.event.stub.internal.TopicManagerAdminServiceEventAdminExceptionException;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.clients.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;
import org.xml.sax.SAXException;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.rmi.RemoteException;

import static org.testng.Assert.assertTrue;

/**
 * This class includes test cases with multiple topics.
 */
public class MultipleTopicTestCase extends MBPlatformBaseTest {

    private static final long EXPECTED_COUNT = 2000L;
    private static final long SEND_COUNT = 2000L;
    private AutomationContext automationContext;
    private TopicAdminClient topicAdminClient;

    /**
     * Prepare environment for tests.
     *
     * @throws LoginAuthenticationExceptionException
     * @throws IOException
     * @throws XPathExpressionException
     * @throws URISyntaxException
     * @throws SAXException
     * @throws XMLStreamException
     */
    @BeforeClass(alwaysRun = true)
    public void init()
            throws LoginAuthenticationExceptionException, IOException, XPathExpressionException,
                   URISyntaxException, SAXException, XMLStreamException {
        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);

        automationContext = getAutomationContextWithKey("mb002");

        topicAdminClient = new TopicAdminClient(automationContext.getContextUrls().getBackEndUrl(),
                                                super.login(automationContext), ConfigurationContextProvider.getInstance().getConfigurationContext());

    }

    /**
     * Publish messages to a topic in a single node and receive from the same node
     *
     * @throws JMSException
     * @throws AndesClientException
     * @throws XPathExpressionException
     * @throws NamingException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "Same node publisher subscriber test case")
    public void testMultipleTopicSingleNode()
            throws JMSException, AndesClientException, XPathExpressionException, NamingException,
                   IOException {
        // Creating receiver clients
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

        // Starting up receiver clients
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

        // Creating publisher clients
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

        // Starting up publisher clients
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

        // Evaluating
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
     * @param topicName     Name of the topic which the subscriber subscribes
     * @param expectedCount Expected message count to be received
     * @return AndesClient object to receive messages
     * @throws NamingException
     * @throws JMSException
     * @throws AndesClientException
     * @throws XPathExpressionException
     */
    private AndesClient getAndesReceiverClient(String topicName, long expectedCount)
            throws NamingException, JMSException, AndesClientException, XPathExpressionException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(automationContext.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, topicName);
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        return new AndesClient(consumerConfig);
    }

    /**
     * Return AndesClient to send messages to a given topic
     *
     * @param topicName Name of the topic
     * @param sendCount Message count to be sent
     * @return An andes client
     * @throws XPathExpressionException
     * @throws AndesClientException
     * @throws NamingException
     * @throws JMSException
     */
    private AndesClient getAndesSenderClient(String topicName, long sendCount)
            throws XPathExpressionException, AndesClientException, NamingException, JMSException {
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, topicName);
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        return new AndesClient(publisherConfig);
    }

    /**
     * Cleanup after running tests.
     *
     * @throws TopicManagerAdminServiceEventAdminExceptionException
     * @throws RemoteException
     */
    @AfterClass(alwaysRun = true)
    public void destroy()
            throws TopicManagerAdminServiceEventAdminExceptionException, RemoteException {

        topicAdminClient.removeTopic("topic1");
        topicAdminClient.removeTopic("topic2");
        topicAdminClient.removeTopic("topic3");
        topicAdminClient.removeTopic("topic4");
        topicAdminClient.removeTopic("topic5");
        topicAdminClient.removeTopic("topic6");
        topicAdminClient.removeTopic("topic7");
        topicAdminClient.removeTopic("topic8");
        topicAdminClient.removeTopic("topic9");
        topicAdminClient.removeTopic("topic10");

    }

}
