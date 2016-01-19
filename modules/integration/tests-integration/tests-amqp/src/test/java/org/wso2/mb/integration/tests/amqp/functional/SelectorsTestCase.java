/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.AndesJMSPublisher;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

/**
 * Test case to test functionality of selectors. Selectors can be used to filter messages received
 * for the consumer.
 */
public class SelectorsTestCase extends MBIntegrationBaseTest {

    /**
     * Message count sent
     */
    private static final long SEND_COUNT = 10L;

    /**
     * Expected message count
     */
    private static final long EXPECTED_COUNT = SEND_COUNT;

    /**
     * Initializes test case
     *
     * @throws XPathExpressionException
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws XPathExpressionException {
        super.init(TestUserMode.SUPER_TENANT_USER);
    }

    /**
     * 1. Create a consumer that accepts messages with JMSType message header value having as AAA
     * 2. Publish messages that does not have JMSType value as AAA
     * 3. Verify that no messages are received by receiver.
     *
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "send-receive test case with jms selectors without conforming messages")
    public void performQueueSendWithReceiverHavingSelectorsButNoModifiedPublisherSelectors()
            throws AndesClientConfigurationException, NamingException, JMSException, IOException,
                   AndesClientException, XPathExpressionException {

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(),
                                                        ExchangeType.QUEUE, "jmsSelectorSubscriberJMSType");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSType='AAA'");
        consumerConfig.setAsync(false);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                                                         ExchangeType.QUEUE, "jmsSelectorSubscriberJMSType");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient
                                    .getSentMessageCount(), SEND_COUNT, "Message sending failed");
        Assert.assertEquals(consumerClient
                                    .getReceivedMessageCount(), 0, "Message receiving failed.");
    }

    /**
     * 1. Create a consumer that accepts messages with JMSType message header value having as AAA
     * 2. Publish messages that does have JMSType value as AAA
     * 3. Verify that all sent messages received by receiver.
     *
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb")
    public void performQueueSendWithModifiedPublisherSelectors()
            throws AndesClientConfigurationException, NamingException, JMSException, IOException,
                   AndesClientException, XPathExpressionException {

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(),
                                                        ExchangeType.QUEUE, "jmsSelectorSubscriberAndPublisherJMSType");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSType='AAA'");
        consumerConfig.setAsync(false);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                                                         ExchangeType.QUEUE, "jmsSelectorSubscriberAndPublisherJMSType");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        long publisherSentMessageCount = 0L;
        while (publisherSentMessageCount < SEND_COUNT) {
            TextMessage textMessage =
                    publisherClient.getPublishers().get(0).getSession().createTextMessage();
            textMessage.setJMSType("AAA");
            publisherClient.getPublishers().get(0).getSender().send(textMessage);
            publisherSentMessageCount++;
        }

        publisherClient.stopClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherSentMessageCount, SEND_COUNT, "Message sending failed");
        Assert.assertEquals(consumerClient
                                    .getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed.");
    }

    /**
     * 1. Create a consumer that accepts message which are published 1 second after the current time.
     * 2. Publisher sends messages with a delay.
     * 3. Consumer will receive a certain amount of messages. But will not receive all messages.
     *
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb")
    public void performQueueSendWithTimestampBasedSelectors()
            throws AndesClientConfigurationException, NamingException, JMSException, IOException,
                   AndesClientException, XPathExpressionException {

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(),
                                                        ExchangeType.QUEUE, "jmsSelectorSubscriberJMSTimestamp");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSTimestamp > " + Long.toString(System.currentTimeMillis() + 1000L));
        consumerConfig.setAsync(false);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                                                         ExchangeType.QUEUE, "jmsSelectorSubscriberJMSTimestamp");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setRunningDelay(300L);  // Setting publishing delay

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient
                                    .getSentMessageCount(), SEND_COUNT, "Message sending failed");
        Assert.assertTrue(consumerClient
                                  .getReceivedMessageCount() < EXPECTED_COUNT, "Message receiving failed.");
    }

    /**
     * 1. Create a consumer that filters out message which has the "location" property as "wso2.trace".
     * 2. 2 publishers will send messages with one having location as "wso2.trace" and another having
     * "wso2.palmgrove".
     * 3. Consumer should only receive messages having "wso2.trace".
     *
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb")
    public void performQueueReceiverCustomPropertyBasedSelectors()
            throws AndesClientConfigurationException, NamingException, JMSException, IOException,
                   AndesClientException, XPathExpressionException {

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(),
                                                        ExchangeType.QUEUE, "jmsSelectorSubscriberCustomProperty");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("location = 'wso2.trace'");
        consumerConfig.setAsync(false);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration initialPublisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                                                         ExchangeType.QUEUE, "jmsSelectorSubscriberCustomProperty");
        AndesJMSPublisherClientConfiguration secondaryPublisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                                                         ExchangeType.QUEUE, "jmsSelectorSubscriberCustomProperty");

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient initialPublisherClient = new AndesClient(initialPublisherConfig, true);
        long initialPublisherSentMessageCount = 0L;
        while (initialPublisherSentMessageCount < SEND_COUNT / 2L) {
            TextMessage textMessage =
                    initialPublisherClient.getPublishers().get(0).getSession().createTextMessage();
            textMessage.setStringProperty("location", "wso2.trace");
            initialPublisherClient.getPublishers().get(0).getSender().send(textMessage);
            initialPublisherSentMessageCount++;
        }

        AndesClient secondaryPublisherClient = new AndesClient(secondaryPublisherConfig, true);
        long secondaryPublisherSentMessageCount = 0L;
        while (secondaryPublisherSentMessageCount < SEND_COUNT / 2L) {
            TextMessage textMessage =
                    initialPublisherClient.getPublishers().get(0).getSession().createTextMessage();
            textMessage.setStringProperty("location", "wso2.palmgrove");
            secondaryPublisherClient.getPublishers().get(0).getSender().send(textMessage);
            secondaryPublisherSentMessageCount++;
        }

        initialPublisherClient.stopClient();
        secondaryPublisherClient.stopClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(initialPublisherSentMessageCount, SEND_COUNT / 2L, "Message sending failed for first client");
        Assert.assertEquals(secondaryPublisherSentMessageCount, SEND_COUNT / 2L, "Message sending failed for second client");
        Assert.assertEquals(consumerClient
                                    .getReceivedMessageCount(), SEND_COUNT / 2L, "Message receiving failed.");

    }

    /**
     * 1. Create consumer that filters out messages having "location" as "wso2.trace" and "JMSType" as "MyMessage".
     * 2. Create 2 publisher. One publisher publishing with messages having "location" as "wso2.trace"
     * and "JMSType" as "MyMessage" in message header. Other having "location" as "wso2.palmGrove"
     * and "JMSType" as "otherMessage" in message header.
     * 3. Consumer should only receive messages having header "location" as "wso2.trace" and "JMSType" as "MyMessage".
     *
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb")
    public void performQueueReceiverCustomPropertyAndJMSTypeBasedSelectors()
            throws AndesClientConfigurationException, NamingException, JMSException, IOException,
                   AndesClientException, XPathExpressionException {

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(),
                                                        ExchangeType.QUEUE,
                                                        "jmsSelectorSubscriberCustomPropertyAndJMSType");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("location = 'wso2.trace' AND JMSType='myMessage'");
        consumerConfig.setAsync(false);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration initialPublisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                                                         ExchangeType.QUEUE,
                                                         "jmsSelectorSubscriberCustomPropertyAndJMSType");
        AndesJMSPublisherClientConfiguration secondaryPublisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                                                         ExchangeType.QUEUE,
                                                         "jmsSelectorSubscriberCustomPropertyAndJMSType");

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient initialPublisherClient = new AndesClient(initialPublisherConfig, true);
        long initialPublisherSentMessageCount = 0L;
        while (initialPublisherSentMessageCount < SEND_COUNT / 2L) {
            TextMessage textMessage =
                    initialPublisherClient.getPublishers().get(0).getSession().createTextMessage();
            textMessage.setJMSType("myMessage");
            textMessage.setStringProperty("location", "wso2.trace");
            initialPublisherClient.getPublishers().get(0).getSender().send(textMessage);
            initialPublisherSentMessageCount++;
        }

        AndesClient secondaryPublisherClient = new AndesClient(secondaryPublisherConfig, true);
        long secondaryPublisherSentMessageCount = 0L;
        while (secondaryPublisherSentMessageCount < SEND_COUNT / 2L) {
            TextMessage textMessage =
                    initialPublisherClient.getPublishers().get(0).getSession().createTextMessage();
            textMessage.setJMSType("otherMessage");
            textMessage.setStringProperty("location", "wso2.palmGrove");
            secondaryPublisherClient.getPublishers().get(0).getSender().send(textMessage);
            secondaryPublisherSentMessageCount++;
        }

        initialPublisherClient.stopClient();
        secondaryPublisherClient.stopClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(initialPublisherSentMessageCount, SEND_COUNT / 2L, "Message sending failed for first client");
        Assert.assertEquals(secondaryPublisherSentMessageCount, SEND_COUNT / 2L, "Message sending failed for second client");
        Assert.assertEquals(consumerClient
                                    .getReceivedMessageCount(), SEND_COUNT / 2L, "Message receiving failed.");
    }

    /**
     * 1. Create consumer that filters out messages having "location" as "wso2.palmgrove" or "JMSType" as "MyMessage".
     * 2. Create 2 publisher. One publisher publishing with messages having "location" as "wso2.trace"
     * and "JMSType" as "MyMessage" in message header. Other having "location" as "wso2.palmGrove"
     * and "JMSType" as "otherMessage" in message header.
     * 3. Consumer should receive all sent messages.
     *
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb")
    public void performQueueReceiverCustomPropertyOrJMSTypeBasedSelectors()
            throws AndesClientConfigurationException, NamingException, JMSException, IOException,
                   AndesClientException, XPathExpressionException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(),
                                                        ExchangeType.QUEUE,
                                                        "jmsSelectorSubscriberCustomPropertyOrJMSType");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("location = 'wso2.palmGrove' OR JMSType='myMessage'");
        consumerConfig.setAsync(false);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration initialPublisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                                                         ExchangeType.QUEUE,
                                                         "jmsSelectorSubscriberCustomPropertyOrJMSType");

        AndesJMSPublisherClientConfiguration secondaryPublisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                                                         ExchangeType.QUEUE,
                                                         "jmsSelectorSubscriberCustomPropertyOrJMSType");

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient initialPublisherClient = new AndesClient(initialPublisherConfig, true);
        long initialPublisherSentMessageCount = 0L;
        while (initialPublisherSentMessageCount < SEND_COUNT / 2L) {
            TextMessage textMessage =
                    initialPublisherClient.getPublishers().get(0).getSession().createTextMessage();
            textMessage.setJMSType("myMessage");
            textMessage.setStringProperty("location", "wso2.trace");
            initialPublisherClient.getPublishers().get(0).getSender().send(textMessage);
            initialPublisherSentMessageCount++;
        }

        AndesClient secondaryPublisherClient = new AndesClient(secondaryPublisherConfig, true);
        long secondaryPublisherSentMessageCount = 0L;
        while (secondaryPublisherSentMessageCount < SEND_COUNT / 2L) {
            TextMessage textMessage =
                    initialPublisherClient.getPublishers().get(0).getSession().createTextMessage();
            textMessage.setJMSType("otherMessage");
            textMessage.setStringProperty("location", "wso2.palmGrove");
            secondaryPublisherClient.getPublishers().get(0).getSender().send(textMessage);
            secondaryPublisherSentMessageCount++;
        }

        initialPublisherClient.stopClient();
        secondaryPublisherClient.stopClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(initialPublisherSentMessageCount, SEND_COUNT / 2L, "Message sending failed for first client");
        Assert.assertEquals(secondaryPublisherSentMessageCount, SEND_COUNT / 2L, "Message sending failed for second client");
        Assert.assertEquals(consumerClient
                                    .getReceivedMessageCount(), SEND_COUNT, "Message receiving failed.");
    }

    /**
     * 1. Create 3 selector subscribers which are subscribed to the same queue with different selectors.
     * 2. Publish messages matching the selectors of the subscribers.
     * 3. Verify whether the three subscribers have received only matching number of messages.
     *
     * @throws AndesClientConfigurationException
     * @throws XPathExpressionException
     * @throws IOException
     * @throws JMSException
     * @throws AndesClientException
     * @throws NamingException
     */
    @Test(groups = "wso2.mb")
    public void performMultipleSelectorSubscribersTestCase() throws AndesClientConfigurationException,
            XPathExpressionException, IOException, JMSException, AndesClientException, NamingException {
        String queueName = "multipleSelectorSubscribers";
        // Creating a consumer client configuration with JMSType selector
        AndesJMSConsumerClientConfiguration jmsTypeSelectorConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(),
                        ExchangeType.QUEUE, queueName);
        jmsTypeSelectorConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        jmsTypeSelectorConfig.setSelectors("JMSType='AAA'");
        jmsTypeSelectorConfig.setAsync(false);

        // Creating a consumer client configuration with 'location' selector
        AndesJMSConsumerClientConfiguration locationSelectorConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(),
                        ExchangeType.QUEUE, queueName);
        locationSelectorConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        locationSelectorConfig.setSelectors("location='wso2.trace'");
        locationSelectorConfig.setAsync(false);

        // Creating a consumer client configuration with 'location' and JMSType selector
        AndesJMSConsumerClientConfiguration jmsTypeAndLocationSelectorConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(),
                        ExchangeType.QUEUE, queueName);
        jmsTypeAndLocationSelectorConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        jmsTypeAndLocationSelectorConfig.setSelectors("location='wso2.pg' AND JMSType='BBB'");
        jmsTypeAndLocationSelectorConfig.setAsync(false);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(),
                        ExchangeType.QUEUE, queueName);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        // Creating clients
        AndesClient jmsTypeClient = new AndesClient(jmsTypeSelectorConfig, true);
        jmsTypeClient.startClient();

        AndesClient locationClient = new AndesClient(locationSelectorConfig, true);
        locationClient.startClient();

        AndesClient jmsTypeAndLocationClient = new AndesClient(jmsTypeAndLocationSelectorConfig, true);
        jmsTypeAndLocationClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        AndesJMSPublisher publisher = publisherClient.getPublishers().get(0);
        MessageProducer messageProducer = publisher.getSender();

        // Send JMSType = AAA messages
        for (int i = 0; i < SEND_COUNT; i++) {
            TextMessage jmsTypeAAAMessage = publisher.getSession().createTextMessage();
            jmsTypeAAAMessage.setJMSType("AAA");
            messageProducer.send(jmsTypeAAAMessage);
        }

        // Send 'location' = 'wso2.trace' messages
        TextMessage locationTraceMessage = publisher.getSession().createTextMessage();
        locationTraceMessage.setStringProperty("location", "wso2.trace");
        for (int i = 0; i < SEND_COUNT; i++) {
            messageProducer.send(locationTraceMessage);
        }

        // Send JMSType = BBB and 'location' = 'wso2.pg' messages
        TextMessage jmsTypeBBBLocationPGMessage = publisher.getSession().createTextMessage();
        jmsTypeBBBLocationPGMessage.setJMSType("BBB");
        jmsTypeBBBLocationPGMessage.setStringProperty("location", "wso2.pg");
        for (int i = 0; i < SEND_COUNT; i++) {
            messageProducer.send(jmsTypeBBBLocationPGMessage);
        }

        publisherClient.stopClient();

        AndesClientUtils.waitForMessagesAndShutdown(jmsTypeClient, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(locationClient, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(jmsTypeAndLocationClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Verify message counts
        Assert.assertEquals(jmsTypeClient.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive expected"
                + " number of message for JMSType = AAA selector client");
        Assert.assertEquals(jmsTypeClient.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive expected"
                + " number of message for 'location' = 'wso2.trace' selector client");
        Assert.assertEquals(jmsTypeClient.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive expected"
                + " number of message for JMSType = BBB and 'location' = 'wso2.pg' selector client");

    }
}
