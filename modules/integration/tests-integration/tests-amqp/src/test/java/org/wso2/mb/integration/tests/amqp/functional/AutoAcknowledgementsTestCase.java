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
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.AndesJMSClient;
import org.wso2.mb.integration.common.clients.AndesJMSConsumerClient;
import org.wso2.mb.integration.common.clients.AndesJMSPublisherClient;
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

    private static final long SEND_COUNT = 1500L;
    private static final long EXPECTED_COUNT = 1500L;

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
        // AUTO_ACKNOWLEDGE = 1;
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);

        // Creating a JMS publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(consumerConfig);
        // Amount of messages to send
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        // Creating clients
        AndesJMSConsumerClient receivingClient = new AndesJMSConsumerClient(consumerConfig);
        receivingClient.startClient();

        AndesJMSPublisherClient sendingClient = new AndesJMSPublisherClient(publisherConfig);
        sendingClient.startClient();

        // Evaluating results
        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(receivingClient, AndesClientConstants.DEFAULT_RUN_TIME);

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
    @Test(groups = "wso2.mb", description = "Single queue send-receive test case with droping the receiving client")
    public void autoAcknowledgementsDropReceiverTestCase()
            throws AndesClientException, CloneNotSupportedException, JMSException, NamingException,
                   IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration initialConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "autoAckTestQueue");
        initialConsumerConfig.setMaximumMessagesToReceived(1000L);
        // Prints per message
        initialConsumerConfig.setPrintsPerMessageCount(100L);

        // Creating a JMS publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(initialConsumerConfig);
        publisherConfig.setRunningDelay(10L);
        // Amount of messages to send
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        // Creating a secondary JMS publisher client configuration
        AndesJMSConsumerClientConfiguration consumerConfigForClientAfterDrop = initialConsumerConfig.clone();
        consumerConfigForClientAfterDrop.setMaximumMessagesToReceived(500L);

        // Creating clients
        AndesJMSConsumerClient initialReceivingClient = new AndesJMSConsumerClient(initialConsumerConfig);
        initialReceivingClient.startClient();

        AndesJMSPublisherClient sendingClient = new AndesJMSPublisherClient(publisherConfig);
        sendingClient.startClient();

        //Wait until messages receive
        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(initialReceivingClient,  AndesClientConstants.DEFAULT_RUN_TIME);
        long totalMessagesReceived = initialReceivingClient.getReceivedMessageCount();

        initialReceivingClient.stopClient();

        // Creating clients
        AndesJMSConsumerClient secondaryReceivingClient = new AndesJMSConsumerClient(consumerConfigForClientAfterDrop);
        secondaryReceivingClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(secondaryReceivingClient,  AndesClientConstants.DEFAULT_RUN_TIME);
        totalMessagesReceived = totalMessagesReceived + secondaryReceivingClient.getReceivedMessageCount();

        //To pass this test received number of messages equals to sent messages
        Assert.assertEquals(sendingClient.getSentMessageCount(), SEND_COUNT, "Messaging sending failed");
        Assert.assertEquals(totalMessagesReceived, EXPECTED_COUNT, "Total number of received messages should be equal to total number of sent messages");






//        //Create receiving client
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:autoAckTestQueue",
//                                                              "100", "false", RUN_TIME.toString(), EXPECTED_COUNT.toString(),
//                                                              "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=1000",
//                                                              "");
//        //Start receiving client
//        receivingClient.startWorking();
//        //Create sending client
//        AndesClientTemp sendingClient =
//                new AndesClientTemp("send", "127.0.0.1:5672", "queue:autoAckTestQueue", "100", "false", RUN_TIME.toString(),
//                                    SEND_COUNT.toString(), "1", "ackMode=1,delayBetweenMsg=10,stopAfter=" + SEND_COUNT, "");
//        //Start sending client
//        sendingClient.startWorking();
//        //Wait until messages receive
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, EXPECTED_COUNT, RUN_TIME);
//        Integer totalMessagesReceived = receivingClient.getReceivedqueueMessagecount();
//        receivingClient.shutDownClient();
//        //Stop receiving client
//        receivingClient.shutDownClient();
//        //Create new receiving client
//        AndesClientTemp receivingClientAfterDrop =
//                new AndesClientTemp("receive", "127.0.0.1:5672", "queue:autoAckTestQueue", "100", "false",
//                                    RUN_TIME.toString(), EXPECTED_COUNT.toString(), "1",
//                                    "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=2000", "");
////        Start new receiving client
//        receivingClientAfterDrop.startWorking();
//        //Wait until messages receive
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClientAfterDrop, EXPECTED_COUNT, 15);
//        totalMessagesReceived = totalMessagesReceived + receivingClientAfterDrop.getReceivedqueueMessagecount();
//        boolean sendSuccess = AndesClientUtilsTemp.getIfPublisherIsSuccess(sendingClient, SEND_COUNT);
//        Assert.assertTrue(sendSuccess, "Messaging sending failed");
//        //To pass this test received number of messages equals to sent messages
//        Assert.assertEquals(totalMessagesReceived, EXPECTED_COUNT,
//                            "Total number of received messages should be equal to total number of sent messages");
    }
}
