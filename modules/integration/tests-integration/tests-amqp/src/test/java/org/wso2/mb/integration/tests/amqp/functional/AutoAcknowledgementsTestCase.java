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
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;

/**
 * This class includes test cases to test auto acknowledgements modes for queues
 */
public class AutoAcknowledgementsTestCase extends MBIntegrationBaseTest {

    /**
     * The amount of messages to be sent.
     */
    private static final long SEND_COUNT = 1500L;

    /**
     * The amount of messages to be expected.
     */
    private static final long EXPECTED_COUNT = SEND_COUNT;

    /**
     * Prepare environment for tests
     *
     * @throws Exception
     */
    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(1000);
    }

    /**
     * In this method we just test a sender and receiver with acknowledgements
     * 1. Start a queue receiver in client ack mode
     * 2. Receive messages acknowledging message bunch to bunch
     * 3. Check whether all messages received
     */
    @Test(groups = "wso2.mb", description = "Single queue send-receive test case with auto Ack")
    public void autoAcknowledgementsTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

        // Creating a JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "autoAckTestQueue");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);

        // Creating a JMS publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(consumerConfig);
        // Amount of messages to send
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

        // Creating clients
        AndesClient receivingClient = new AndesClient(consumerConfig);
        receivingClient.startClient();

        AndesClient sendingClient = new AndesClient(publisherConfig);
        sendingClient.startClient();

        // Evaluating results
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(sendingClient.getSentMessageCount(), SEND_COUNT, "Messaging sending failed");
        Assert.assertEquals(receivingClient.getReceivedMessageCount(), EXPECTED_COUNT, "Total number of sent and received messages are not equal");
    }

    /**
     * In this method we drop receiving client and connect it again and tries to get messages from MB
     * 1. Start a queue receiver in client ack mode
     * 2. Receive messages acking message bunch to bunch
     * 3. Drop the queue receiver
     * 4. Start a another queue receiver in client ack mode
     * 5. Check whether total received messages were equal to send messages
     */
    @Test(groups = "wso2.mb", description = "Single queue send-receive test case with dropping the receiving client")
    public void autoAcknowledgementsDropReceiverTestCase()
            throws AndesClientException, CloneNotSupportedException, JMSException, NamingException,
                   IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration initialConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "autoAckTestQueueDropReceiver");
        initialConsumerConfig.setMaximumMessagesToReceived(1000L);
        // Prints per message
        initialConsumerConfig.setPrintsPerMessageCount(1000L / 10L);

        // Creating a JMS publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "autoAckTestQueueDropReceiver");
        //publisherConfig.setRunningDelay(10L);
        // Amount of messages to send
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

        // Creating clients
        AndesClient initialReceivingClient = new AndesClient(initialConsumerConfig);
        initialReceivingClient.startClient();

        AndesClient sendingClient = new AndesClient(publisherConfig);
        sendingClient.startClient();

        //Wait until messages receive
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(initialReceivingClient, AndesClientConstants.DEFAULT_RUN_TIME);
        long totalMessagesReceived = initialReceivingClient.getReceivedMessageCount();

        log.info("Messages received by initialClient so far : " + totalMessagesReceived);

        // Creating a secondary JMS publisher client configuration
        AndesJMSConsumerClientConfiguration consumerConfigForClientAfterDrop = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "autoAckTestQueueDropReceiver");
        consumerConfigForClientAfterDrop.setMaximumMessagesToReceived(EXPECTED_COUNT - 1000L);


        // Creating clients
        AndesClient secondaryReceivingClient = new AndesClient(consumerConfigForClientAfterDrop);
        secondaryReceivingClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(secondaryReceivingClient, AndesClientConstants.DEFAULT_RUN_TIME);
        totalMessagesReceived = totalMessagesReceived + secondaryReceivingClient.getReceivedMessageCount();

        //To pass this test received number of messages equals to sent messages
        Assert.assertEquals(sendingClient.getSentMessageCount(), SEND_COUNT, "Messaging sending failed");
        Assert.assertEquals(totalMessagesReceived, EXPECTED_COUNT, "Total number of received messages should be equal to total number of sent messages");

    }
}
