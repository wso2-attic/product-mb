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
 * This class tests broker with multiple publisher and subscribers
 */
public class MultipleSubscriberMultiplePublisherTestCase extends MBPlatformBaseTest {

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
     * Multiple subscribers and publishers in same node for a single queue
     *
     * @throws XPathExpressionException
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "Same node single queue multiple subscriber " +
                                            "publisher test case")
    public void testSameNodeSingleQueueMultipleSubscriberPublisher() throws
                                                                     XPathExpressionException,
                                                                     AndesClientConfigurationException,
                                                                     NamingException, JMSException,
                                                                     IOException {
        // Number of messages expected
        long expectedCount = 250L;
        // Number of messages send
        long sendCount = 250L;

        String brokerURL = getRandomAMQPBrokerUrl();

        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(brokerURL, ExchangeType.QUEUE, "singleQueue1");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(brokerURL, ExchangeType.QUEUE, "singleQueue1");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient1 = new AndesClient(consumerConfig, true);
        consumerClient1.startClient();
        AndesClient consumerClient2 = new AndesClient(consumerConfig, true);
        consumerClient2.startClient();
        AndesClient consumerClient3 = new AndesClient(consumerConfig, true);
        consumerClient3.startClient();
        AndesClient consumerClient4 = new AndesClient(consumerConfig, true);
        consumerClient4.startClient();

        AndesClient publisherClient1 = new AndesClient(publisherConfig, true);
        publisherClient1.startClient();
        AndesClient publisherClient2 = new AndesClient(publisherConfig, true);
        publisherClient2.startClient();
        AndesClient publisherClient3 = new AndesClient(publisherConfig, true);
        publisherClient3.startClient();
        AndesClient publisherClient4 = new AndesClient(publisherConfig, true);
        publisherClient4.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient3, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient4, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient1.getSentMessageCount(), sendCount, "Message sending failed by publisherClient1.");
        Assert.assertEquals(publisherClient2.getSentMessageCount(), sendCount, "Message sending failed by publisherClient2.");
        Assert.assertEquals(publisherClient3.getSentMessageCount(), sendCount, "Message sending failed by publisherClient3.");
        Assert.assertEquals(publisherClient4.getSentMessageCount(), sendCount, "Message sending failed by publisherClient4.");
        Assert.assertEquals(consumerClient1.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient1.");
        Assert.assertEquals(consumerClient2.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient2.");
        Assert.assertEquals(consumerClient3.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient3.");
        Assert.assertEquals(consumerClient4.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient4.");

        long totalMessagesSent = publisherClient1.getSentMessageCount() + publisherClient2.getSentMessageCount() + publisherClient3.getSentMessageCount() + publisherClient4.getSentMessageCount();
        long totalMessagesReceived = consumerClient1.getSentMessageCount() + consumerClient2.getSentMessageCount() + consumerClient3.getSentMessageCount() + consumerClient4.getSentMessageCount();
        Assert.assertEquals(totalMessagesSent, totalMessagesReceived, "Message receiving failed by all consumers");
        Assert.assertEquals(totalMessagesSent, sendCount * 4, "Message receiving by all consumers does not match the message count that was sent");
    }

    /**
     * Multiple subscribers and publishers in Multiple node for a single queue
     *
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws XPathExpressionException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "Multiple node single queue multiple subscriber " +
                                            "publisher test case")
    public void testMultiNodeSingleQueueMultipleSubscriberPublisher()
            throws AndesClientConfigurationException, XPathExpressionException, NamingException, JMSException,
                   IOException {
        // Number of messages expected
        long expectedCount = 250L;
        // Number of messages send
        long sendCount = 250L;

        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(getRandomAMQPBrokerUrl(), ExchangeType.QUEUE, "singleQueue2");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(getRandomAMQPBrokerUrl(), ExchangeType.QUEUE, "singleQueue2");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient1 = new AndesClient(consumerConfig, true);
        consumerClient1.startClient();

        consumerConfig.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient consumerClient2 = new AndesClient(consumerConfig, true);
        consumerClient2.startClient();

        consumerConfig.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient consumerClient3 = new AndesClient(consumerConfig, true);
        consumerClient3.startClient();

        consumerConfig.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient consumerClient4 = new AndesClient(consumerConfig, true);
        consumerClient4.startClient();

        AndesClient publisherClient1 = new AndesClient(publisherConfig, true);
        publisherClient1.startClient();

        publisherConfig.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient publisherClient2 = new AndesClient(publisherConfig, true);
        publisherClient2.startClient();

        publisherConfig.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient publisherClient3 = new AndesClient(publisherConfig, true);
        publisherClient3.startClient();

        publisherConfig.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient publisherClient4 = new AndesClient(publisherConfig, true);
        publisherClient4.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient3, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient4, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient1.getSentMessageCount(), sendCount, "Message sending failed by publisherClient1.");
        Assert.assertEquals(publisherClient2.getSentMessageCount(), sendCount, "Message sending failed by publisherClient2.");
        Assert.assertEquals(publisherClient3.getSentMessageCount(), sendCount, "Message sending failed by publisherClient3.");
        Assert.assertEquals(publisherClient4.getSentMessageCount(), sendCount, "Message sending failed by publisherClient4.");
        Assert.assertEquals(consumerClient1.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient1.");
        Assert.assertEquals(consumerClient2.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient2.");
        Assert.assertEquals(consumerClient3.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient3.");
        Assert.assertEquals(consumerClient4.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient4.");

        long totalMessagesSent = publisherClient1.getSentMessageCount() + publisherClient2.getSentMessageCount() + publisherClient3.getSentMessageCount() + publisherClient4.getSentMessageCount();
        long totalMessagesReceived = consumerClient1.getSentMessageCount() + consumerClient2.getSentMessageCount() + consumerClient3.getSentMessageCount() + consumerClient4.getSentMessageCount();
        Assert.assertEquals(totalMessagesSent, totalMessagesReceived, "Message receiving failed by all consumers");
        Assert.assertEquals(totalMessagesSent, sendCount * 4, "Message receiving by all consumers does not match the message count that was sent");
    }

    /**
     * Multiple subscribers and publishers in Multiple node for Multiple queues
     *
     * @throws org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws XPathExpressionException
     * @throws IOException
     * @throws CloneNotSupportedException
     */
    @Test(groups = "wso2.mb", description = "Multiple node Multiple queue multiple subscriber " +
                                            "publisher test case")
    public void testMultiNodeMultipleQueueMultipleSubscriberPublisher() throws
                                                                        AndesClientConfigurationException,
                                                                        NamingException,
                                                                        JMSException,
                                                                        XPathExpressionException,
                                                                        IOException,
                                                                        CloneNotSupportedException {
        // Number of messages expected
        long expectedCount = 250L;
        // Number of messages send
        long sendCount = 250L;

        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(getRandomAMQPBrokerUrl(), ExchangeType.QUEUE, "singleQueue3");
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(getRandomAMQPBrokerUrl(), ExchangeType.QUEUE, "singleQueue3");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesClient consumerClient1 = new AndesClient(consumerConfig, true);
        consumerClient1.startClient();

        AndesJMSConsumerClientConfiguration consumerConfig2 = consumerConfig.clone();
        consumerConfig2.setDestinationName("singleQueue4");
        consumerConfig2.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient consumerClient2 = new AndesClient(consumerConfig2, true);
        consumerClient2.startClient();

        AndesJMSConsumerClientConfiguration consumerConfig3 = consumerConfig.clone();
        consumerConfig3.setDestinationName("singleQueue5");
        consumerConfig3.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient consumerClient3 = new AndesClient(consumerConfig3, true);
        consumerClient3.startClient();

        AndesJMSConsumerClientConfiguration consumerConfig4 = consumerConfig.clone();
        consumerConfig4.setDestinationName("singleQueue6");
        consumerConfig4.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient consumerClient4 = new AndesClient(consumerConfig4, true);
        consumerClient4.startClient();

        AndesClient publisherClient1 = new AndesClient(publisherConfig, true);
        publisherClient1.startClient();

        AndesJMSPublisherClientConfiguration publisherConfig2 = publisherConfig.clone();
        publisherConfig2.setDestinationName("singleQueue4");
        publisherConfig2.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient publisherClient2 = new AndesClient(publisherConfig2, true);
        publisherClient2.startClient();

        AndesJMSPublisherClientConfiguration publisherConfig3 = publisherConfig.clone();
        publisherConfig3.setDestinationName("singleQueue5");
        publisherConfig3.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient publisherClient3 = new AndesClient(publisherConfig3, true);
        publisherClient3.startClient();

        AndesJMSPublisherClientConfiguration publisherConfig4 = publisherConfig.clone();
        publisherConfig4.setDestinationName("singleQueue6");
        publisherConfig4.setConnectionString(getRandomAMQPBrokerUrl());
        AndesClient publisherClient4 = new AndesClient(publisherConfig4, true);
        publisherClient4.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient3, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient4, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient1.getSentMessageCount(), sendCount, "Message sending failed by publisherClient1.");
        Assert.assertEquals(publisherClient2.getSentMessageCount(), sendCount, "Message sending failed by publisherClient2.");
        Assert.assertEquals(publisherClient3.getSentMessageCount(), sendCount, "Message sending failed by publisherClient3.");
        Assert.assertEquals(publisherClient4.getSentMessageCount(), sendCount, "Message sending failed by publisherClient4.");
        Assert.assertEquals(consumerClient1.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient1.");
        Assert.assertEquals(consumerClient2.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient2.");
        Assert.assertEquals(consumerClient3.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient3.");
        Assert.assertEquals(consumerClient4.getReceivedMessageCount(), expectedCount, "Message receiving failed by consumerClient4.");

        long totalMessagesSent = publisherClient1.getSentMessageCount() + publisherClient2.getSentMessageCount() + publisherClient3.getSentMessageCount() + publisherClient4.getSentMessageCount();
        long totalMessagesReceived = consumerClient1.getSentMessageCount() + consumerClient2.getSentMessageCount() + consumerClient3.getSentMessageCount() + consumerClient4.getSentMessageCount();
        Assert.assertEquals(totalMessagesSent, totalMessagesReceived, "Message receiving failed by all consumers");
        Assert.assertEquals(totalMessagesSent, sendCount * 4, "Message receiving by all consumers does not match the message count that was sent");
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

        if (tempAndesAdminClient.getQueueByName("singleQueue3") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue3");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueue4") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue4");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueue5") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue5");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueue6") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue6");
        }
    }
}