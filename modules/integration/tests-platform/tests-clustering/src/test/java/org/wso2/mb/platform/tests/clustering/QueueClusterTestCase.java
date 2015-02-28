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

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.andes.stub.admin.types.Queue;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.clients.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException;
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
 * Test class to test queues in clusters
 */
public class QueueClusterTestCase extends MBPlatformBaseTest {

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
     * Send and receive messages in a single node for a queue
     *
     * @throws XPathExpressionException
     * @throws AndesAdminServiceBrokerManagerAdminException
     * @throws IOException
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     */
    @Test(groups = "wso2.mb", description = "Single queue Single node send-receive test case")
    public void testSingleQueueSingleNodeSendReceive() throws XPathExpressionException,
                                                              AndesAdminServiceBrokerManagerAdminException,
                                                              IOException,
                                                              AndesClientConfigurationException, NamingException,
                                                              JMSException {

        long sendCount = 1000L;
        long expectedCount = 1000L;

        String randomInstanceKey = getRandomMBInstance();

        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(tempContext.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(tempContext.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.QUEUE, "clusterSingleQueue1");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);


        randomInstanceKey = getRandomMBInstance();
        Queue queue = getAndesAdminClientWithKey(randomInstanceKey).getQueueByName("clusterSingleQueue1");
        assertTrue(queue.getQueueName().equalsIgnoreCase("clusterSingleQueue1"), "Queue created in MB node 1 not exist");

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(tempContext.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(tempContext.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.QUEUE, "clusterSingleQueue1");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }

    /**
     * Creating the same queue in 2 different nodes.
     *
     * @throws AndesAdminServiceBrokerManagerAdminException
     * @throws RemoteException
     */
    @Test(groups = "wso2.mb", description = "Single queue replication")
    public void testSingleQueueReplication()
            throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        String queueName = "singleQueue2";

        String randomInstanceKey = getRandomMBInstance();

        AndesAdminClient tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);

        if (tempAndesAdminClient.getQueueByName(queueName) != null) {
            tempAndesAdminClient.deleteQueue(queueName);
        }

        tempAndesAdminClient.createQueue(queueName);

        randomInstanceKey = getRandomMBInstance();
        tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);
        Queue queue = tempAndesAdminClient.getQueueByName(queueName);

        assertTrue(queue != null && queue.getQueueName().equalsIgnoreCase(queueName),
                   "Queue created in MB node instance not replicated in other MB node instance");

        tempAndesAdminClient.deleteQueue(queueName);
        randomInstanceKey = getRandomMBInstance();
        tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);
        queue = tempAndesAdminClient.getQueueByName(queueName);

        assertTrue(queue == null,
                   "Queue created in MB node instance not replicated in other MB node instance");

    }

    /**
     * Send messages from one node and received messages from another node.
     *
     * @throws XPathExpressionException
     * @throws AndesAdminServiceBrokerManagerAdminException
     * @throws IOException
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     */
    @Test(groups = "wso2.mb", description = "Single queue Multi node send-receive test case")
    public void testSingleQueueMultiNodeSendReceive() throws XPathExpressionException,
                                                             AndesAdminServiceBrokerManagerAdminException,
                                                             IOException,
                                                             AndesClientConfigurationException,
                                                             NamingException, JMSException {
        long sendCount = 1000L;
        long expectedCount = 1000L;

        String randomInstanceKey = getRandomMBInstance();

        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(tempContext.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(tempContext.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.QUEUE, "clusterSingleQueue3");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);


        randomInstanceKey = getRandomMBInstance();
        tempContext = getAutomationContextWithKey(randomInstanceKey);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(tempContext.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(tempContext.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.QUEUE, "clusterSingleQueue3");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

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

        if (tempAndesAdminClient.getQueueByName("clusterSingleQueue1") != null) {
            tempAndesAdminClient.deleteQueue("clusterSingleQueue1");
        }

        if (tempAndesAdminClient.getQueueByName("clusterSingleQueue2") != null) {
            tempAndesAdminClient.deleteQueue("clusterSingleQueue2");
        }

        if (tempAndesAdminClient.getQueueByName("clusterSingleQueue3") != null) {
            tempAndesAdminClient.deleteQueue("clusterSingleQueue3");
        }
    }

}
