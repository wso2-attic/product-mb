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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class includes test cases to test client acknowledgements modes for queues
 */
public class ClientAcknowledgementsTestCase extends MBIntegrationBaseTest {

    /**
     * Amount of messages sent.
     */
    private static final long SEND_COUNT = 1000L;

    /**
     * Amount of messages expected.
     */
    private static final long EXPECTED_COUNT = SEND_COUNT;

    /**
     * Initializing test case
     *
     * @throws XPathExpressionException
     */
    @BeforeClass
    public void prepare() throws XPathExpressionException {
        super.init(TestUserMode.SUPER_TENANT_USER);
    }

    /**
     * In this test it will check functionality of client acknowledgement by acknowledging bunch by
     * bunch.
     * 1. Start queue receiver in client acknowledge mode.
     * 2. Publisher sends {@link #SEND_COUNT} messages.
     * 3. Consumer receives messages and only acknowledge after each 200 messages.
     * 4. Consumer should receive {@link #EXPECTED_COUNT} messages.
     *
     * @throws AndesClientConfigurationException
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = {"wso2.mb", "queue"})
    public void performClientAcknowledgementsTestCase()
            throws AndesClientConfigurationException, JMSException, NamingException, IOException,
                   AndesClientException, XPathExpressionException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(), ExchangeType.QUEUE, "clientAckTestQueue");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig
                .setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE); // using client acknowledgement
        consumerConfig
                .setAcknowledgeAfterEachMessageCount(200L); // acknowledge a message only after 200 messages are received
        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);
        consumerConfig.setAsync(false);

        // Creating a JMS publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(), ExchangeType.QUEUE, "clientAckTestQueue");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

        AndesClient consumerClient1 = new AndesClient(consumerConfig, true);
        consumerClient1.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);

        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000);

        long totalMessagesReceived = consumerClient1.getReceivedMessageCount() + consumerClient
                .getReceivedMessageCount();

        Assert.assertEquals(publisherClient
                                    .getSentMessageCount(), SEND_COUNT, "Expected message count not sent.");
        Assert.assertEquals(totalMessagesReceived, EXPECTED_COUNT, "Expected message count not received.");
    }

    /**
     * In this test, client acknowledgement will be tested with no consumer cache for queues. Implying that connection
     * and sessions are not cached.
     * 1. Create a consumer with client acknowledgement that take one message and then closes the client.
     * 2. Publish a message to queue.
     * 3. Create 20 consumers with client acknowledgement that takes one message and closes itself consecutively.
     * 4. Here the consumers does not ack at all.
     *
     * @throws AndesClientConfigurationException
     * @throws JMSException
     * @throws AndesClientException
     * @throws NamingException
     * @throws IOException
     * @throws XPathExpressionException
     */
    @Test(groups = {"wso2.mb", "queue"})
    public void performClientAckWithNoCacheQueueTestCase() throws AndesClientConfigurationException, JMSException,
            AndesClientException, NamingException, IOException, XPathExpressionException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(), ExchangeType.QUEUE, "clientAckTestQueueNoCache");
        consumerConfig.setMaximumMessagesToReceived(1);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE); // using client acknowledgement
        consumerConfig.setAcknowledgeAfterEachMessageCount(200L); // acknowledge a message only after 200 messages are received
        consumerConfig.setAsync(false);

        // Creating a JMS publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                                                                    ExchangeType.QUEUE, "clientAckTestQueueNoCache");
        publisherConfig.setNumberOfMessagesToSend(1);

        // Start consumer
        AndesClient consumerClient1 = new AndesClient(consumerConfig, true);
        consumerClient1.startClient();

        // Start publisher
        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        // Waiting for messages to come.
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient1, 3000);

        // Eval first consumer
        Assert.assertEquals(consumerClient1.getReceivedMessageCount(), 1, "Expected message count not received.");

        // Create 20 more consumers that take in one message and closes it self. No ack.
        for (int i = 0; i <= 20; i++) {
            AndesClient consumerClient = new AndesClient(consumerConfig, true);
            consumerClient.startClient();
            AndesClientUtils.waitForMessagesAndShutdown(consumerClient, 3000);
            Assert.assertEquals(consumerClient.getReceivedMessageCount(), 1, "Expected message count not " +
                                                                                           "received.");

        }

        // Evaluating the publishers.
        Assert.assertEquals(publisherClient.getSentMessageCount(), 1, "Expected message count not received.");
    }

    /**
     * In this test, client acknowledgement will be tested with no consumer cache for durable topics. Implying that
     * connection and sessions are not cached.
     * 1. Create 10 consumers with client acknowledgement.
     * 2. Publish a message to the durable topic.
     * 3. Close the earlier 10 consumers after taking one message without acking.
     * 4. Restart the same consumers
     * 5. Again close the earlier 10 consumers after taking one message without acking.
     * 6. Restart the same consumers
     * 7. Create same 10 consumers with client acknowledgement that takes one message and closes itself consecutively
     * with ack.
     *
     * @throws AndesClientConfigurationException
     * @throws JMSException
     * @throws AndesClientException
     * @throws NamingException
     * @throws IOException
     * @throws XPathExpressionException
     */
    @Test(groups = {"wso2.mb", "queue"})
    public void performClientAckWithNoCacheDurableTopicTestCase() throws AndesClientConfigurationException, JMSException,
            AndesClientException, NamingException, IOException, XPathExpressionException, CloneNotSupportedException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(getAMQPPort(),
                                                        ExchangeType.TOPIC, "clientAckTestDurableNoCache");
        consumerConfig.setMaximumMessagesToReceived(1);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE); // using client acknowledgement
        consumerConfig.setAcknowledgeAfterEachMessageCount(200L); // acknowledge a message only after 200 messages are
                                                                                                            // received
        consumerConfig.setAsync(false);

        // Creating a JMS publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                ExchangeType.TOPIC, "clientAckTestDurableNoCache");
        publisherConfig.setNumberOfMessagesToSend(1);

        List<AndesClient> initialClients = new ArrayList<>();

        // Create 10 more consumers that take in one message and closes it self. No ack.
        for (int i = 0; i <= 10; i++) {
            AndesJMSConsumerClientConfiguration consumerConfig2 = consumerConfig.clone();
            consumerConfig2.setDurable(true, "ack-client-" + Integer.toString(i));
            AndesClient consumerClient = new AndesClient(consumerConfig2, true);
            consumerClient.startClient();
            initialClients.add(consumerClient);
        }

        // Start publisher
        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        // Wait for each consumer to get one message and close themselves without acking.
        for (AndesClient initialClient : initialClients) {
            AndesClientUtils.waitForMessagesAndShutdown(initialClient, 3000);
            Assert.assertEquals(initialClient.getReceivedMessageCount(), 1, "Expected message count not " +
                              "received for " + initialClient.getConsumers().get(0).getConfig().getSubscriptionID());
        }

        // Restart the same clients.
        for (AndesClient initialClient : initialClients) {
            AndesJMSConsumerClientConfiguration config = initialClient.getConsumers().get(0).getConfig();
            AndesClient consumerClient = new AndesClient(config, true);
            consumerClient.startClient();
        }

        // Wait for each consumer to get one message and close themselves without acking.
        for (AndesClient initialClient : initialClients) {
            AndesClientUtils.waitForMessagesAndShutdown(initialClient, 3000);
            Assert.assertEquals(initialClient.getReceivedMessageCount(), 1, "Expected message count not " +
                                "received for " + initialClient.getConsumers().get(0).getConfig().getSubscriptionID());
        }

        // Create same 10 consumers that take in one message and closes it self. With ack.
        for (int i = 0; i <= 10; i++) {
            AndesJMSConsumerClientConfiguration consumerConfig2 = consumerConfig.clone();
            consumerConfig2.setMaximumMessagesToReceived(5L);
            consumerConfig2.setAcknowledgeAfterEachMessageCount(1L);
            consumerConfig2.setDurable(true, "ack-client-" + Integer.toString(i));
            AndesClient consumerClient = new AndesClient(consumerConfig2, true);
            consumerClient.startClient();
            AndesClientUtils.waitForMessagesAndShutdown(consumerClient, 3000);
            Assert.assertEquals(consumerClient.getReceivedMessageCount(), 1, "Expected message count not " +
                                                              "received for " + consumerConfig2.getSubscriptionID());

        }

        // Evaluating the publishers.
        Assert.assertEquals(publisherClient.getSentMessageCount(), 1, "Expected message count not received.");
    }
}
