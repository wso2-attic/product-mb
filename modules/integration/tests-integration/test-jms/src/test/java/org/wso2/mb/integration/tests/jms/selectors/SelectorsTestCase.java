/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.mb.integration.tests.jms.selectors;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.andes.client.message.JMSTextMessage;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageHeader;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;
import org.wso2.mb.integration.tests.JMSTestConstants;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SelectorsTestCase extends MBIntegrationBaseTest {
    private static final long SEND_COUNT = 10L;
    private static final long EXPECTED_COUNT = SEND_COUNT;


    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
    }

    /**
     * 1. Subscribe to a queue with selectors.
     * 2. Send messages without jms type as configured
     * 3. Verify that 0 messages received by receiver
     */
    @Test(groups = "wso2.mb", description = "send-receive test case with jms selectors without conforming messages")
    public void performQueueSendWithReceiverHavingSelectorsButNoModifiedPublisherSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSType");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSType='AAA'");

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSType");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), 0, "Message receiving failed.");



//        AndesClient receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
//                                                      JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(),
//                                                      "0", "1", "listener=true,ackMode=1,delayBetweenMsg=" +
//                                                                JMSTestConstants.STANDARD_DELAY_BETWEEN_MESSAGES +
//                                                                ",stopAfter=" + 0 + ",jMSSelector=JMSType='AAA'", "");
//
//        receivingClient.startWorking();
//
//        AndesClient sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
//                                                    JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(),
//                                                    JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT.toString(), "1",
//                                                    "ackMode=1,delayBetweenMsg=" +
//                                                    JMSTestConstants.STANDARD_DELAY_BETWEEN_MESSAGES + ",stopAfter=" +
//                                                    JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, 0, 20);
//
//        boolean sendSuccess =
//                AndesClientUtils.getIfPublisherIsSuccess(sendingClient, JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT);
//
//        assertTrue(receiveSuccess, "Message receiving failed");
//        assertTrue(sendSuccess, "Message sending failed.");
    }

    @Test(groups = "wso2.mb", description = "send-receive test case with jms selectors without conforming messages")
    public void performQueueSendWithModifiedPublisherSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberAndPublisherJMSType");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSType='AAA'");

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberAndPublisherJMSType");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        JMSMessageHeader jmsMessageHeader = new JMSMessageHeader();
        jmsMessageHeader.setJmsType("AAA");
        publisherConfig.setMessageHeader(jmsMessageHeader);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed.");
    }

    @Test(groups = "wso2.mb", description = "send-receive test case with jms selectors without conforming messages")
    public void performQueueSendWithTimestampBasedSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSTimestamp");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSTimestamp > " + Long.toString(System.currentTimeMillis() + 1000L));

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSTimestamp");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setRunningDelay(300L);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed");
        Assert.assertTrue(consumerClient.getReceivedMessageCount() < EXPECTED_COUNT, "Message receiving failed.");
    }

    @Test(groups = "wso2.mb", description = "send-receive test case with jms selectors without conforming messages")
    public void performQueueReceiverPriorityBasedSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSPriority");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSPriority=9");

        AndesJMSPublisherClientConfiguration initialPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSPriority");
        initialPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT / 2L);
        JMSMessageHeader jmsMessageHeader = new JMSMessageHeader();
        jmsMessageHeader.setJmsPriority(9);
        initialPublisherConfig.setMessageHeader(jmsMessageHeader);

        AndesJMSPublisherClientConfiguration secondaryPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSPriority");
        secondaryPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT/2L);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient initialPublisherClient = new AndesClient(initialPublisherConfig);
        initialPublisherClient.startClient();

        AndesClient secondaryPublisherClient = new AndesClient(secondaryPublisherConfig);
        secondaryPublisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(initialPublisherClient.getSentMessageCount(), SEND_COUNT/2L, "Message sending failed");
        Assert.assertEquals(secondaryPublisherClient.getSentMessageCount(), SEND_COUNT/2L, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), SEND_COUNT/2L, "Message receiving failed.");

    }

    @Test(groups = "wso2.mb", description = "send-receive test case with jms selectors without conforming messages")
    public void performQueueReceiverDeliverTypeBasedSelectors()
            throws AndesClientException, NamingException, JMSException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSDelivery");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setSelectors("JMSDeliveryMode='NON_PERSISTENT'");

        AndesJMSPublisherClientConfiguration initialPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSDelivery");
        initialPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT / 2L);
        JMSMessageHeader jmsMessageHeader = new JMSMessageHeader();
        jmsMessageHeader.setJmsDeliveryMode(DeliveryMode.NON_PERSISTENT);
        initialPublisherConfig.setMessageHeader(jmsMessageHeader);

        AndesJMSPublisherClientConfiguration secondaryPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSDelivery");
        secondaryPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT/2L);
        jmsMessageHeader.setJmsDeliveryMode(DeliveryMode.PERSISTENT);
        initialPublisherConfig.setMessageHeader(jmsMessageHeader);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient initialPublisherClient = new AndesClient(initialPublisherConfig);
        initialPublisherClient.startClient();

        AndesClient secondaryPublisherClient = new AndesClient(secondaryPublisherConfig);
        secondaryPublisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(initialPublisherClient.getSentMessageCount(), SEND_COUNT/2L, "Message sending failed");
        Assert.assertEquals(secondaryPublisherClient.getSentMessageCount(), SEND_COUNT/2L, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), SEND_COUNT/2L, "Message receiving failed.");

    }

//    @Test(groups = "wso2.mb", description = "send-receive test case with jms selectors without conforming messages")
//    public void performQueueReceiverDeliverTypeAndPriorityBasedSelectors()
//            throws AndesClientException, NamingException, JMSException, IOException {
//
//        // Creating a initial JMS consumer client configuration
//        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSDelivery");
//        // Amount of message to receive
//        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
//        consumerConfig.setSelectors("JMSDeliveryMode = 2");
//
//        AndesJMSPublisherClientConfiguration initialPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSDelivery");
//        initialPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT / 2L);
//        JMSMessageHeader jmsMessageHeader = new JMSMessageHeader();
//        jmsMessageHeader.setJmsDeliveryMode(DeliveryMode.NON_PERSISTENT);
//        initialPublisherConfig.setMessageHeader(jmsMessageHeader);
//
//        AndesJMSPublisherClientConfiguration secondaryPublisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "jmsSelectorSubscriberJMSDelivery");
//        secondaryPublisherConfig.setNumberOfMessagesToSend(SEND_COUNT/2L);
//        jmsMessageHeader.setJmsDeliveryMode(DeliveryMode.PERSISTENT);
//        initialPublisherConfig.setMessageHeader(jmsMessageHeader);
//
//        AndesClient consumerClient = new AndesClient(consumerConfig);
//        consumerClient.startClient();
//
//        AndesClient initialPublisherClient = new AndesClient(initialPublisherConfig);
//        initialPublisherClient.startClient();
//
//        AndesClient secondaryPublisherClient = new AndesClient(secondaryPublisherConfig);
//        secondaryPublisherClient.startClient();
//
//        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
//        Assert.assertEquals(initialPublisherClient.getSentMessageCount(), SEND_COUNT/2L, "Message sending failed");
//        Assert.assertEquals(secondaryPublisherClient.getSentMessageCount(), SEND_COUNT/2L, "Message sending failed");
//        Assert.assertEquals(consumerClient.getReceivedMessageCount(), SEND_COUNT/2L, "Message receiving failed.");
//
//    }
}
