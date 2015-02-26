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

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.rmi.RemoteException;

/**
 * This class includes all order guaranteeing tests
 */
public class OrderGuaranteeTestCase extends MBPlatformBaseTest {

    /**
     * Prepare environment for tests.
     *
     * @throws Exception
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);
        super.initAndesAdminClients();
    }

    /**
     * Publish message to a single node and receive from the same node and check for any out of
     * order delivery and message duplication.
     *
     * @throws XPathExpressionException
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "Same node ordered delivery test case")
    public void testSameNodeOrderedDelivery() throws XPathExpressionException,
                                                     AndesClientConfigurationException, NamingException,
                                                     JMSException, IOException {
        // Number of messages expected
        long expectedCount = 1000L;
        // Number of messages send
        long sendCount = 1000L;

        String brokerUrl = getRandomAMQPBrokerUrl();

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(brokerUrl, ExchangeType.QUEUE, "singleQueueOrder1");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);
        consumerConfig.setFilePathToWriteReceivedMessages(AndesClientConstants.FILE_PATH_TO_WRITE_RECEIVED_MESSAGES);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(brokerUrl, ExchangeType.QUEUE, "singleQueueOrder1");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");

        // Evaluating
        Assert.assertTrue(consumerClient.checkIfMessagesAreInOrder(), "Messages did not receive in order.");
        Assert.assertEquals(consumerClient.checkIfMessagesAreDuplicated().size(), 0, "Messages are not duplicated.");
    }

    /**
     * Publish message to a single node and receive from another node and check for any out of order
     * delivery and message duplication.
     *
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws XPathExpressionException
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "Different node ordered delivery test case")
    public void testDifferentNodeOrderedDelivery()
            throws AndesClientConfigurationException, XPathExpressionException, JMSException, NamingException,
                   IOException {
        // Number of messages expected
        long expectedCount = 1000L;
        // Number of messages send
        long sendCount = 1000L;

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(getRandomAMQPBrokerUrl(), ExchangeType.QUEUE, "singleQueueOrder2");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);
        consumerConfig.setFilePathToWriteReceivedMessages(AndesClientConstants.FILE_PATH_TO_WRITE_RECEIVED_MESSAGES);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(getRandomAMQPBrokerUrl(), ExchangeType.QUEUE, "singleQueueOrder2");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");

        // Evaluating
        Assert.assertTrue(consumerClient.checkIfMessagesAreInOrder(), "Messages did not receive in order.");
        Assert.assertEquals(consumerClient.checkIfMessagesAreDuplicated().size(), 0, "Messages are not duplicated.");
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

        if (tempAndesAdminClient.getQueueByName("singleQueue1") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue1");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueue2") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue2");
        }
    }
}