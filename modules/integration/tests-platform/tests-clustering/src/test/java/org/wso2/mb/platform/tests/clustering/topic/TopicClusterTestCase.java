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
import org.wso2.carbon.event.stub.internal.xsd.TopicNode;
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
 * Test class to test topics in clusters
 */
public class TopicClusterTestCase extends MBPlatformBaseTest {

    private AutomationContext automationContextForMB2;
    private AutomationContext automationContextForMB3;
    private TopicAdminClient topicAdminClientForMB2;
    private TopicAdminClient topicAdminClientForMB3;

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

        automationContextForMB2 = getAutomationContextWithKey("mb002");
        automationContextForMB3 = getAutomationContextWithKey("mb003");

        topicAdminClientForMB2 = new TopicAdminClient(automationContextForMB2.getContextUrls().getBackEndUrl(),
          super.login(automationContextForMB2), ConfigurationContextProvider.getInstance().getConfigurationContext());

        topicAdminClientForMB3 = new TopicAdminClient(automationContextForMB3.getContextUrls().getBackEndUrl(),
          super.login(automationContextForMB3), ConfigurationContextProvider.getInstance().getConfigurationContext());
    }

    /**
     * Send and receive messages in a single node for a topic
     *
     * @throws AndesClientConfigurationException
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     * @throws TopicManagerAdminServiceEventAdminExceptionException
     * @throws XPathExpressionException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "Single topic Single node send-receive test case")
    public void testSingleTopicSingleNodeSendReceive()
            throws AndesClientConfigurationException, JMSException, NamingException, IOException,
                   TopicManagerAdminServiceEventAdminExceptionException, XPathExpressionException,
                   AndesClientException {
        long sendCount = 1000L;
        long expectedCount = 1000L;

        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(automationContextForMB2.getInstance().getHosts().get("default"),
                        Integer.parseInt(automationContextForMB2.getInstance().getPorts().get("amqp")),
                        ExchangeType.TOPIC, "clusterSingleTopic1");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(automationContextForMB2.getInstance().getHosts().get("default"),
                        Integer.parseInt(automationContextForMB2.getInstance().getPorts().get("amqp")),
                        ExchangeType.TOPIC, "clusterSingleTopic1");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        TopicNode topic = topicAdminClientForMB2.getTopicByName("clusterSingleTopic1");
        assertTrue(topic.getTopicName().equalsIgnoreCase("clusterSingleTopic1"), "Topic created in" +
                                                                             " MB node 1 not exist");

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }

    /**
     * Checking for topic deletion and adding cluster wide.
     *
     * @throws TopicManagerAdminServiceEventAdminExceptionException
     * @throws RemoteException
     */
    @Test(groups = "wso2.mb", description = "Single topic replication")
    public void testSingleTopicReplication()
            throws TopicManagerAdminServiceEventAdminExceptionException, RemoteException {

        String topic = "singleTopic2";

        topicAdminClientForMB2.addTopic(topic);
        TopicNode topicNode = topicAdminClientForMB3.getTopicByName(topic);

        assertTrue(topicNode != null && topicNode.getTopicName().equalsIgnoreCase(topic),
                   "Topic created in MB node 1 not replicated in MB node 2");

        topicAdminClientForMB3.removeTopic(topic);
        topicNode = topicAdminClientForMB3.getTopicByName(topic);

        assertTrue(topicNode == null,
                   "Topic deleted in MB node 2 not deleted in MB node 1");

    }

    /**
     * Send messages from one node and received messages from another node.
     *
     * @throws AndesClientConfigurationException
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     * @throws TopicManagerAdminServiceEventAdminExceptionException
     * @throws XPathExpressionException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "Single topic Multi node send-receive test case")
    public void testSingleTopicMultiNodeSendReceive()
            throws AndesClientConfigurationException, JMSException, NamingException, IOException,
                   TopicManagerAdminServiceEventAdminExceptionException, XPathExpressionException,
                   AndesClientException {
        long sendCount = 1000L;
        long expectedCount = 1000L;

        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(automationContextForMB2.getInstance().getHosts().get("default"),
                     Integer.parseInt(automationContextForMB2.getInstance().getPorts().get("amqp")),
                     ExchangeType.TOPIC, "clusterSingleTopic3");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);


        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(automationContextForMB3.getInstance().getHosts().get("default"),
                    Integer.parseInt(automationContextForMB3.getInstance().getPorts().get("amqp")),
                    ExchangeType.TOPIC, "clusterSingleTopic3");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        TopicNode topic = topicAdminClientForMB2.getTopicByName("clusterSingleTopic3");
        assertTrue(topic.getTopicName().equalsIgnoreCase("clusterSingleTopic3"), "Topic created in MB node 1 not exist");

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
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

        topicAdminClientForMB2.removeTopic("clusterSingleTopic1");
        topicAdminClientForMB2.removeTopic("clusterSingleTopic2");
        topicAdminClientForMB2.removeTopic("clusterSingleTopic3");
    }
}
