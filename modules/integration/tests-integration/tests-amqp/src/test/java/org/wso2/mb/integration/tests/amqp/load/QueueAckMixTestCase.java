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

package org.wso2.mb.integration.tests.amqp.load;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.queue.QueueMessageReceiver;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.jms.QueueSession;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Load test for standalone MB.
 */
public class QueueAckMixTestCase extends MBIntegrationBaseTest {

    private static final long SEND_COUNT = 100000L;
    private static final long EXPECTED_COUNT = SEND_COUNT;
    private static final int NUMBER_OF_SUBSCRIBERS = 7;
    private static final int NUMBER_OF_PUBLISHERS = 7;
    private static final long NUMBER_OF_RETURNED_MESSAGES = SEND_COUNT / 10L;
    private static final int NUMBER_OF_CLIENT_ACK_SUBSCRIBERS = 1;
    private static final int NUMBER_OF_AUTO_ACK_SUBSCRIBERS = NUMBER_OF_SUBSCRIBERS - NUMBER_OF_CLIENT_ACK_SUBSCRIBERS;

    // Greater than send count to see if more than the sent amount is received
//    private Integer expectedCount = sendCount;

    /**
     * Initialize the test as super tenant user.
     *
     * @throws Exception
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * Send million messages and Receive them via AUTO_ACKNOWLEDGE subscribers and CLIENT_ACKNOWLEDGE subscribers and
     * check if AUTO_ACKNOWLEDGE subscribers receive all the messages.
     */
    @Test(groups = "wso2.mb", description = "Send million messages and Receive them via AUTO_ACKNOWLEDGE subscribers " +
                                            "and CLIENT_ACKNOWLEDGE", enabled = true)
    public void performMillionMessageTenPercentReturnTestCase()
            throws AndesClientException, NamingException, JMSException, IOException {


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "MillionTenPercentAckMixReturnQueue");
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.AUTO_ACKNOWLEDGE);
        consumerConfig.setAcknowledgeAfterEachMessageCount(200);
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerReturnedConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "MillionTenPercentAckMixReturnQueue");
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE);
        consumerConfig.setAcknowledgeAfterEachMessageCount(100000);
        // Amount of message to receive
        consumerReturnedConfig.setMaximumMessagesToReceived(NUMBER_OF_RETURNED_MESSAGES);
        // Prints per message
        consumerReturnedConfig.setPrintsPerMessageCount(NUMBER_OF_RETURNED_MESSAGES / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "MillionTenPercentAckMixReturnQueue");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig, NUMBER_OF_AUTO_ACK_SUBSCRIBERS);
        consumerClient.startClient();

        AndesClient consumerReturnedClient = new AndesClient(consumerReturnedConfig, NUMBER_OF_CLIENT_ACK_SUBSCRIBERS);
        consumerReturnedClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, NUMBER_OF_PUBLISHERS);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerReturnedClient, AndesClientConstants.DEFAULT_RUN_TIME);

        long totalReceivedMessageCount = consumerClient.getReceivedMessageCount() + consumerReturnedClient.getReceivedMessageCount();

        log.info("Total Non Returning Subscribers Received Messages [" + consumerClient.getReceivedMessageCount() + "]");
        log.info("Total Returning Subscribers Received Messages [" + consumerReturnedClient.getReceivedMessageCount() + "]");
        log.info("Total Received Messages [" + totalReceivedMessageCount + "]");

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT * NUMBER_OF_PUBLISHERS, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), SEND_COUNT, "Did not receive expected message count.");




//         Integer sendCount = 100000;
//         Integer runTime = 30 * 15; // 15 minutes
//         Integer noOfSubscribers = 7;
//         Integer noOfPublishers = 7;
//
//        // Greater than send count to see if more than the sent amount is received
//         Integer expectedCount = sendCount;
//
//
//        Integer noOfReturnMessages = sendCount / 100;
//        Integer noOfClientAckSubscribers = 1;
//        Integer noOfAutoAckSubscribers = noOfSubscribers - noOfClientAckSubscribers;
//
//        String queueNameArg = "queue:MillionTenPercentReturnQueue";
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", queueNameArg,
//                                                              "100", "false", runTime.toString(), expectedCount.toString(),
//                                                              noOfAutoAckSubscribers.toString(), "listener=true,ackMode=" + QueueSession.AUTO_ACKNOWLEDGE + "," +
//                                                                                                 "delayBetweenMsg=0,ackAfterEach=200,stopAfter=" + expectedCount, "");
//
//        AndesClientTemp receivingReturnClient = new AndesClientTemp("receive", "127.0.0.1:5672", queueNameArg,
//                                                                    "100", "false", runTime.toString(), noOfReturnMessages.toString(),
//                                                                    noOfClientAckSubscribers.toString(), "listener=true,ackMode=" + QueueSession.CLIENT_ACKNOWLEDGE + "," +
//                                                                                                         "ackAfterEach=100000,delayBetweenMsg=0,stopAfter=" + noOfReturnMessages, "");
//
//        receivingClient.startWorking();
//        receivingReturnClient.startWorking();
//
//        List<QueueMessageReceiver> autoAckListeners = receivingClient.getQueueListeners();
//        List<QueueMessageReceiver> clientAckListeners = receivingReturnClient.getQueueListeners();
//        log.info("Number of AUTO ACK Subscriber [" + autoAckListeners.size() + "]");
//        log.info("Number of CLIENT ACK Subscriber [" + clientAckListeners.size() + "]");
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", queueNameArg, "100", "false",
//                                                            runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
//                                                            "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        AndesClientUtilsTemp.waitUntilAllMessagesReceived(receivingClient, "MillionTenPercentReturnQueue", expectedCount,
//                                                          runTime);
//
//        AndesClientUtilsTemp.waitUntilExactNumberOfMessagesReceived(receivingReturnClient,
//                                                                    "MillionTenPercentReturnQueue", noOfReturnMessages, runTime);
//
//        AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount() + receivingReturnClient
//                .getReceivedqueueMessagecount();
//
//        log.info("Total AUTO ACK Subscriber Received Messages [" + receivingClient.getReceivedqueueMessagecount() +
//                 "]");
//        log.info("Total CLIENT ACK Subscriber Received Messages [" + receivingReturnClient
//                .getReceivedqueueMessagecount() + "]");
//        log.info("Total Received Messages [" + actualReceivedCount + "]");
//
//        assertEquals(receivingClient.getReceivedqueueMessagecount(), sendCount.intValue(),
//                     "Did not receive expected message count.");
    }
}
