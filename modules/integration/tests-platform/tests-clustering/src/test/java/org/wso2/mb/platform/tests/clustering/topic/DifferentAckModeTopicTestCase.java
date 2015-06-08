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
import org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.clients.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;
import org.xml.sax.SAXException;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.rmi.RemoteException;

/**
 * This class includes test cases to test different ack modes for topics
 */
public class DifferentAckModeTopicTestCase extends MBPlatformBaseTest {
    private AutomationContext automationContext;
    private TopicAdminClient topicAdminClient;

    /**
     * Prepare environment for tests.
     *
     * @throws XPathExpressionException
     * @throws URISyntaxException
     * @throws SAXException
     * @throws XMLStreamException
     * @throws LoginAuthenticationExceptionException
     * @throws IOException
     */
    @BeforeClass(alwaysRun = true)
    public void init()
            throws XPathExpressionException, URISyntaxException, SAXException, XMLStreamException,
            LoginAuthenticationExceptionException, IOException, AutomationUtilException {
        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);

        automationContext = getAutomationContextWithKey("mb002");

        topicAdminClient = new TopicAdminClient(automationContext.getContextUrls().getBackEndUrl(),
                super.login(automationContext), ConfigurationContextProvider.getInstance().getConfigurationContext());
    }

    /**
     * Publish messages to a topic in single node and receive from the same node with
     * session_transacted acknowledge mode
     *
     * @throws XPathExpressionException
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "SESSION_TRANSACTED ack mode test case for topic", enabled = true)
    public void testSessionTransactedAckModeForTopic()
            throws XPathExpressionException, AndesClientConfigurationException, NamingException,
                   JMSException,
                   IOException, AndesClientException {
        // Expected message count
        long expectedCount = 2000L;
        // Number of messages send
        long sendCount = 2000L;

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(automationContext.getInstance().getHosts()
                                                                .get("default"),
                                                        Integer.parseInt(automationContext
                                                                                 .getInstance()
                                                                                 .getPorts()
                                                                                 .get("amqp")),
                                                        ExchangeType.TOPIC, "sessionTransactedAckTopic");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.SESSION_TRANSACTED);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(automationContext.getInstance().getHosts()
                                                                 .get("default"),
                                                         Integer.parseInt(automationContext
                                                                                  .getInstance()
                                                                                  .getPorts()
                                                                                  .get("amqp")),
                                                         ExchangeType.TOPIC, "sessionTransactedAckTopic");
        publisherConfig.setNumberOfMessagesToSend(sendCount);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient
                                    .getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient
                                    .getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }

    /**
     * Publish messages to a topic in single node and receive from the same node with
     * auto acknowledge mode
     *
     * @throws XPathExpressionException
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "AUTO_ACKNOWLEDGE ack mode test case for topic", enabled = true)
    public void testAutoAcknowledgeModeForTopic()
            throws XPathExpressionException, AndesClientConfigurationException, NamingException,
                   JMSException,
                   IOException, AndesClientException {
        // Expected message count
        long expectedCount = 2000L;
        // Number of messages send
        long sendCount = 2000L;

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(automationContext.getInstance().getHosts()
                                                                .get("default"),
                                                        Integer.parseInt(automationContext
                                                                                 .getInstance()
                                                                                 .getPorts()
                                                                                 .get("amqp")),
                                                        ExchangeType.TOPIC, "autoAcknowledgeTopic");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.AUTO_ACKNOWLEDGE);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(automationContext.getInstance().getHosts()
                                                                 .get("default"),
                                                         Integer.parseInt(automationContext
                                                                                  .getInstance()
                                                                                  .getPorts()
                                                                                  .get("amqp")),
                                                         ExchangeType.TOPIC, "autoAcknowledgeTopic");
        publisherConfig.setNumberOfMessagesToSend(sendCount);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient
                                    .getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient
                                    .getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }

    /**
     * Publish messages to a topic in single node and receive from the same node with client
     * acknowledge mode
     *
     * @throws XPathExpressionException
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "CLIENT_ACKNOWLEDGE ack mode test case for topic", enabled = true)
    public void testClientAcknowledgeModeForTopic()
            throws XPathExpressionException, AndesClientConfigurationException, NamingException,
                   JMSException,
                   IOException, AndesClientException {
        // Expected message count
        int expectedCount = 2000;
        // Number of messages send
        int sendCount = 2000;

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(automationContext.getInstance().getHosts()
                                                                .get("default"),
                                                        Integer.parseInt(automationContext
                                                                                 .getInstance()
                                                                                 .getPorts()
                                                                                 .get("amqp")),
                                                        ExchangeType.TOPIC, "clientAcknowledgeTopic");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(automationContext.getInstance().getHosts()
                                                                 .get("default"),
                                                         Integer.parseInt(automationContext
                                                                                  .getInstance()
                                                                                  .getPorts()
                                                                                  .get("amqp")),
                                                         ExchangeType.TOPIC, "clientAcknowledgeTopic");
        publisherConfig.setNumberOfMessagesToSend(sendCount);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient
                                    .getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient
                                    .getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }

    /**
     * Publish messages to a topic in single node and receive from the same node with
     * duplicate acknowledge mode
     *
     * @throws XPathExpressionException
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "DUPS_OK_ACKNOWLEDGE ack mode test case for topic", enabled = true)
    public void testDupOkAcknowledgeModeForTopic()
            throws XPathExpressionException, AndesClientConfigurationException, NamingException,
                   JMSException,
                   IOException, AndesClientException {
        long expectedCount = 2000L;
        long sendCount = 2000L;

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(automationContext.getInstance().getHosts()
                                                                .get("default"),
                                                        Integer.parseInt(automationContext
                                                                                 .getInstance()
                                                                                 .getPorts()
                                                                                 .get("amqp")),
                                                        ExchangeType.TOPIC, "dupsOkAcknowledgeTopic");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.DUPS_OK_ACKNOWLEDGE);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(automationContext.getInstance().getHosts()
                                                                 .get("default"),
                                                         Integer.parseInt(automationContext
                                                                                  .getInstance()
                                                                                  .getPorts()
                                                                                  .get("amqp")),
                                                         ExchangeType.TOPIC, "dupsOkAcknowledgeTopic");
        publisherConfig.setNumberOfMessagesToSend(sendCount);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient
                                    .getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient
                                    .getReceivedMessageCount(), expectedCount, "Message receiving failed.");
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
        topicAdminClient.removeTopic("sessionTransactedAckTopic");
        topicAdminClient.removeTopic("autoAcknowledgeTopic");
        topicAdminClient.removeTopic("clientAcknowledgeTopic");
        topicAdminClient.removeTopic("dupsOkAcknowledgeTopic");
    }
}
