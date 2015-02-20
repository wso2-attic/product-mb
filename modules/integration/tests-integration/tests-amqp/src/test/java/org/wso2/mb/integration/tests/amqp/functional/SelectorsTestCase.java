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
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageHeader;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
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
     * @throws AndesClientException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "send-receive test case with jms selectors without conforming messages")
    public void performQueueSendWithReceiverHavingSelectorsButNoModifiedPublisherSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSType");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSType='AAA'");

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSType");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), 0, "Message receiving failed.");
    }

    /**
     * 1. Create a consumer that accepts messages with JMSType message header value having as AAA
     * 2. Publish messages that does have JMSType value as AAA
     * 3. Verify that all sent messages received by receiver.
     *
     * @throws AndesClientException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb")
    public void performQueueSendWithModifiedPublisherSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberAndPublisherJMSType");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSType='AAA'");

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberAndPublisherJMSType");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        JMSMessageHeader jmsMessageHeader = new JMSMessageHeader();
        jmsMessageHeader.setJmsType("AAA");
        publisherConfig.setMessageHeader(jmsMessageHeader);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed.");
    }

    /**
     * 1. Create a consumer that accepts message which are published 1 second after the current time.
     * 2. Publisher sends messages with a delay.
     * 3. Consumer will receive a certain amount of messages. But will not receive all messages.
     *
     * @throws AndesClientException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb")
    public void performQueueSendWithTimestampBasedSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSTimestamp");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSTimestamp > " + Long.toString(System.currentTimeMillis() + 1000L));

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSTimestamp");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setRunningDelay(300L);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed");
        Assert.assertTrue(consumerClient.getReceivedMessageCount() < EXPECTED_COUNT, "Message receiving failed.");
    }

    /**
     * 1. Create a consumer that filters out message which has the "location" property as "wso2.trace".
     * 2. 2 publishers will send messages with one having location as "wso2.trace" and another having
     * "wso2.palmgrove".
     * 3. Consumer should only receive messages having "wso2.trace".
     *
     * @throws AndesClientException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb")
    public void performQueueReceiverCustomPropertyBasedSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberCustomProperty");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("location = 'wso2.trace'");

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration initialPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberCustomProperty");
        initialPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT / 2L);
        JMSMessageHeader jmsMessageHeaderForInitialPublisher = new JMSMessageHeader();
        jmsMessageHeaderForInitialPublisher.getStringProperties().put("location", "wso2.trace");
        initialPublisherConfig.setMessageHeader(jmsMessageHeaderForInitialPublisher);

        AndesJMSPublisherClientConfiguration secondaryPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberCustomProperty");
        secondaryPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT / 2L);
        JMSMessageHeader jmsMessageHeaderSecondaryPublisher = new JMSMessageHeader();
        jmsMessageHeaderSecondaryPublisher.getStringProperties().put("location", "wso2.palmgrove");
        initialPublisherConfig.setMessageHeader(jmsMessageHeaderSecondaryPublisher);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient initialPublisherClient = new AndesClient(initialPublisherConfig);
        initialPublisherClient.startClient();

        AndesClient secondaryPublisherClient = new AndesClient(secondaryPublisherConfig);
        secondaryPublisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(initialPublisherClient.getSentMessageCount(), SEND_COUNT / 2L, "Message sending failed");
        Assert.assertEquals(secondaryPublisherClient.getSentMessageCount(), SEND_COUNT / 2L, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), SEND_COUNT / 2L, "Message receiving failed.");

    }

    /**
     * 1. Create consumer that filters out messages having "location" as "wso2.trace" and "JMSType" as "MyMessage".
     * 2. Create 2 publisher. One publisher publishing with messages having "location" as "wso2.trace"
     * and "JMSType" as "MyMessage" in message header. Other having "location" as "wso2.palmGrove"
     * and "JMSType" as "otherMessage" in message header.
     * 3. Consumer should only receive messages having header "location" as "wso2.trace" and "JMSType" as "MyMessage".
     *
     * @throws AndesClientException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb")
    public void performQueueReceiverCustomPropertyAndJMSTypeBasedSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberCustomPropertyAndJMSType");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("location = 'wso2.trace' AND JMSType='myMessage'");

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration initialPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberCustomPropertyAndJMSType");
        initialPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT / 2L);
        JMSMessageHeader jmsMessageHeaderForInitialPublisher = new JMSMessageHeader();
        jmsMessageHeaderForInitialPublisher.setJmsType("myMessage");
        jmsMessageHeaderForInitialPublisher.getStringProperties().put("location", "wso2.trace");
        initialPublisherConfig.setMessageHeader(jmsMessageHeaderForInitialPublisher);

        AndesJMSPublisherClientConfiguration secondaryPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberCustomPropertyAndJMSType");
        secondaryPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT / 2L);
        JMSMessageHeader jmsMessageHeaderSecondaryPublisher = new JMSMessageHeader();
        jmsMessageHeaderForInitialPublisher.setJmsType("otherMessage");
        jmsMessageHeaderSecondaryPublisher.getStringProperties().put("location", "wso2.palmGrove");
        initialPublisherConfig.setMessageHeader(jmsMessageHeaderSecondaryPublisher);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient initialPublisherClient = new AndesClient(initialPublisherConfig);
        initialPublisherClient.startClient();

        AndesClient secondaryPublisherClient = new AndesClient(secondaryPublisherConfig);
        secondaryPublisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(initialPublisherClient.getSentMessageCount(), SEND_COUNT / 2L, "Message sending failed");
        Assert.assertEquals(secondaryPublisherClient.getSentMessageCount(), SEND_COUNT / 2L, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), SEND_COUNT / 2L, "Message receiving failed.");
    }

    /**
     * 1. Create consumer that filters out messages having "location" as "wso2.palmgrove" or "JMSType" as "MyMessage".
     * 2. Create 2 publisher. One publisher publishing with messages having "location" as "wso2.trace"
     * and "JMSType" as "MyMessage" in message header. Other having "location" as "wso2.palmGrove"
     * and "JMSType" as "otherMessage" in message header.
     * 3. Consumer should receive all sent messages.
     *
     * @throws AndesClientException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     */
    @Test(groups = "wso2.mb")
    public void performQueueReceiverCustomPropertyOrJMSTypeBasedSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberCustomPropertyOrJMSType");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("location = 'wso2.palmgrove' OR JMSType='myMessage'");

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration initialPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberCustomPropertyOrJMSType");
        initialPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT / 2L);
        JMSMessageHeader jmsMessageHeaderForInitialPublisher = new JMSMessageHeader();
        jmsMessageHeaderForInitialPublisher.setJmsType("myMessage");
        jmsMessageHeaderForInitialPublisher.getStringProperties().put("location", "wso2.trace");
        initialPublisherConfig.setMessageHeader(jmsMessageHeaderForInitialPublisher);

        AndesJMSPublisherClientConfiguration secondaryPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberCustomPropertyOrJMSType");
        secondaryPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT / 2L);
        JMSMessageHeader jmsMessageHeaderSecondaryPublisher = new JMSMessageHeader();
        jmsMessageHeaderForInitialPublisher.setJmsType("otherMessage");
        jmsMessageHeaderSecondaryPublisher.getStringProperties().put("location", "wso2.palmGrove");
        initialPublisherConfig.setMessageHeader(jmsMessageHeaderSecondaryPublisher);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient initialPublisherClient = new AndesClient(initialPublisherConfig);
        initialPublisherClient.startClient();

        AndesClient secondaryPublisherClient = new AndesClient(secondaryPublisherConfig);
        secondaryPublisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(initialPublisherClient.getSentMessageCount(), SEND_COUNT / 2L, "Message sending failed");
        Assert.assertEquals(secondaryPublisherClient.getSentMessageCount(), SEND_COUNT / 2L, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), SEND_COUNT, "Message receiving failed.");
    }
}
