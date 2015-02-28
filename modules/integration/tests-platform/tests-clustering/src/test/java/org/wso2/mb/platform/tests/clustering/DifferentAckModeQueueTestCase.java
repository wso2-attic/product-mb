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

package org.wso2.mb.platform.tests.clustering;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.clients.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException;
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
 * This class includes test cases to test different ack modes for queues
 */
public class DifferentAckModeQueueTestCase extends MBPlatformBaseTest {

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
        super.initAndesAdminClients();
    }

    /**
     * Publish messages to a single node and receive from the same node with SESSION_TRANSACTED
     * ack mode
     *
     * @throws XPathExpressionException
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "SESSION_TRANSACTED ack mode test case for queue")
    public void testSessionTransactedAckModeForQueueTestCase()
            throws XPathExpressionException, AndesClientConfigurationException, NamingException, JMSException,
                   IOException {
        // Expected message count
        int expectedCount = 2000;
        // Number of messages send
        int sendCount = 2000;

        String brokerAddress = getRandomAMQPBrokerAddress();

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(brokerAddress.split(":")[0], Integer.parseInt(brokerAddress.split(":")[1]), ExchangeType.QUEUE, "sessionTransactedAckQueue");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.SESSION_TRANSACTED);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(brokerAddress.split(":")[0], Integer.parseInt(brokerAddress.split(":")[1]), ExchangeType.QUEUE, "sessionTransactedAckQueue");
        publisherConfig.setNumberOfMessagesToSend(sendCount);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }

    /**
     * Publish messages to a single node and receive from the same node with AUTO_ACKNOWLEDGE
     * ack mode
     *
     * @throws XPathExpressionException
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "AUTO_ACKNOWLEDGE ack mode test case for queue")
    public void testAutoAcknowledgeModeForQueue()
            throws XPathExpressionException, AndesClientConfigurationException, NamingException, JMSException,
                   IOException {
        // Expected message count
        int expectedCount = 2000;
        // Number of messages send
        int sendCount = 2000;

        String brokerAddress = getRandomAMQPBrokerAddress();

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(brokerAddress.split(":")[0], Integer.parseInt(brokerAddress.split(":")[1]), ExchangeType.QUEUE, "autoAcknowledgeQueue");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(brokerAddress.split(":")[0], Integer.parseInt(brokerAddress.split(":")[1]), ExchangeType.QUEUE, "autoAcknowledgeQueue");
        publisherConfig.setNumberOfMessagesToSend(sendCount);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }

    /**
     * Publish messages to a single node and receive from the same node with CLIENT_ACKNOWLEDGE
     * ack mode
     *
     * @throws XPathExpressionException
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "CLIENT_ACKNOWLEDGE ack mode test case for queue")
    public void testClientAcknowledgeModeForQueue()
            throws XPathExpressionException, AndesClientConfigurationException, NamingException, JMSException,
                   IOException {
        // Expected message count
        int expectedCount = 2000;
        // Number of messages send
        int sendCount = 2000;

        String brokerAddress = getRandomAMQPBrokerAddress();

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(brokerAddress.split(":")[0], Integer.parseInt(brokerAddress.split(":")[1]), ExchangeType.QUEUE, "clientAcknowledgeQueue");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE);
        consumerConfig.setRunningDelay(10L);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(brokerAddress.split(":")[0], Integer.parseInt(brokerAddress.split(":")[1]), ExchangeType.QUEUE, "clientAcknowledgeQueue");
        publisherConfig.setNumberOfMessagesToSend(sendCount);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }

    /**
     * Publish messages to a single node and receive from the same node with DUPS_OK_ACKNOWLEDGE
     * ack mode
     *
     * @throws XPathExpressionException
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "DUPS_OK_ACKNOWLEDGE ack mode test case for queue")
    public void testDupOkAcknowledgeModeForQueue()
            throws XPathExpressionException, AndesClientConfigurationException, JMSException, NamingException,
                   IOException {
        // Expected message count
        int expectedCount = 2000;
        // Number of messages send
        int sendCount = 2000;

        String brokerAddress = getRandomAMQPBrokerAddress();

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(brokerAddress.split(":")[0], Integer.parseInt(brokerAddress.split(":")[1]), ExchangeType.QUEUE, "dupsOkAcknowledgeQueue");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.DUPS_OK_ACKNOWLEDGE);
        consumerConfig.setRunningDelay(10L);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(brokerAddress.split(":")[0], Integer.parseInt(brokerAddress.split(":")[1]), ExchangeType.QUEUE, "dupsOkAcknowledgeQueue");
        publisherConfig.setNumberOfMessagesToSend(sendCount);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }

    /**
     * Cleanup after running tests.
     *
     * @throws AndesAdminServiceBrokerManagerAdminException
     * @throws RemoteException
     */
    @AfterClass(alwaysRun = true)
    public void destroy() throws AndesAdminServiceBrokerManagerAdminException, RemoteException {

        String randomInstanceKey = getRandomMBInstance();

        AndesAdminClient tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);

        if (tempAndesAdminClient.getQueueByName("sessionTransactedAckQueue") != null) {
            tempAndesAdminClient.deleteQueue("sessionTransactedAckQueue");
        }

        if (tempAndesAdminClient.getQueueByName("autoAcknowledgeQueue") != null) {
            tempAndesAdminClient.deleteQueue("autoAcknowledgeQueue");
        }

        if (tempAndesAdminClient.getQueueByName("clientAcknowledgeQueue") != null) {
            tempAndesAdminClient.deleteQueue("clientAcknowledgeQueue");
        }

        if (tempAndesAdminClient.getQueueByName("dupsOkAcknowledgeQueue") != null) {
            tempAndesAdminClient.deleteQueue("dupsOkAcknowledgeQueue");
        }
    }

}
