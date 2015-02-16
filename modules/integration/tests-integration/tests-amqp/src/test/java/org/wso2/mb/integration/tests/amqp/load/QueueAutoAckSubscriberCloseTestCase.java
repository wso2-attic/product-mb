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
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.List;

/**
 * Load test for standalone MB.
 */
public class QueueAutoAckSubscriberCloseTestCase extends MBIntegrationBaseTest {

    private static final long SEND_COUNT = 100000L;
    private static final long EXPECTED_COUNT = SEND_COUNT;
    private static final int NUMBER_OF_SUBSCRIBERS = 7;
    private static final int NUMBER_OF_PUBLISHERS = 7;
    private static final long NUMBER_OF_MESSAGES_TO_RECEIVE_BY_CLOSING_SUBSCRIBERS = 1000L;
    private static final int NUMBER_OF_SUBSCRIBERS_TO_CLOSE = 1;
    private static final long NUMBER_OF_MESSAGES_TO_EXPECT = EXPECTED_COUNT - NUMBER_OF_MESSAGES_TO_RECEIVE_BY_CLOSING_SUBSCRIBERS;
    private static final int NUMBER_OF_NON_CLOSING_SUBSCRIBERS = NUMBER_OF_SUBSCRIBERS - NUMBER_OF_SUBSCRIBERS_TO_CLOSE;


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
     * Create 50 subscriptions for a queue and publish one million messages. Then close 10% of the subscribers while
     * messages are retrieving and check if all the messages are received by other subscribers.
     */
    @Test(groups = "wso2.mb", description = "50 subscriptions for a queue and 50 publishers. Then close " +
                                            "10% of the subscribers ", enabled = true)
    public void performMillionMessageTenPercentSubscriberCloseTestCase()
            throws AndesClientException, NamingException, JMSException, IOException {


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "millionTenPercentAutoAckSubscriberCloseQueue");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(NUMBER_OF_MESSAGES_TO_EXPECT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(NUMBER_OF_MESSAGES_TO_EXPECT / 10L);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerClosingConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "millionTenPercentAutoAckSubscriberCloseQueue");
        // Amount of message to receive
        consumerClosingConfig.setMaximumMessagesToReceived(NUMBER_OF_MESSAGES_TO_RECEIVE_BY_CLOSING_SUBSCRIBERS);
        // Prints per message
        consumerClosingConfig.setPrintsPerMessageCount(NUMBER_OF_MESSAGES_TO_RECEIVE_BY_CLOSING_SUBSCRIBERS / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "millionTenPercentAutoAckSubscriberCloseQueue");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig, NUMBER_OF_NON_CLOSING_SUBSCRIBERS);
        consumerClient.startClient();

        AndesClient consumerClosingClient = new AndesClient(consumerClosingConfig, NUMBER_OF_SUBSCRIBERS_TO_CLOSE);
        consumerClosingClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, NUMBER_OF_PUBLISHERS);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClosingClient, AndesClientConstants.DEFAULT_RUN_TIME);

        long totalReceivedMessageCount = consumerClient.getReceivedMessageCount() + consumerClosingClient.getReceivedMessageCount();

        log.info("Total Non Closing Subscribers Received Messages [" + consumerClient.getReceivedMessageCount() + "]");
        log.info("Total Closing Subscribers Received Messages [" + consumerClosingClient.getReceivedMessageCount() + "]");
        log.info("Total Received Messages [" + totalReceivedMessageCount + "]");

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT * NUMBER_OF_PUBLISHERS, "Message sending failed");
        Assert.assertTrue(totalReceivedMessageCount >= SEND_COUNT, "Received only " + totalReceivedMessageCount + " messages.");




//        Integer noOfMessagesToReceiveByClosingSubscribers = 1000;
//        Integer noOfSubscribersToClose = 1;
//        Integer noOfMessagesToExpect = expectedCount - noOfMessagesToReceiveByClosingSubscribers;
//        Integer noOfNonClosingSubscribers = noOfSubscribers - noOfSubscribersToClose;
//
//        String queueNameArg = "queue:MillionTenPercentSubscriberCloseQueue";
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", queueNameArg,
//                                                              "100", "false", runTime.toString(), noOfMessagesToExpect.toString(),
//                                                              noOfNonClosingSubscribers.toString(), "listener=true,ackMode=1,delayBetweenMsg=0," +
//                                                                                                    "stopAfter=" + expectedCount, "");
//
//        AndesClientTemp receivingClosingClient = new AndesClientTemp("receive", "127.0.0.1:5672", queueNameArg,
//                                                                     "100", "false", runTime.toString(), noOfMessagesToReceiveByClosingSubscribers.toString(),
//                                                                     noOfSubscribersToClose.toString(), "listener=true,ackMode=1,delayBetweenMsg=0," +
//                                                                                                        "stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//        receivingClosingClient.startWorking();
//
//        List<QueueMessageReceiver> queueListeners = receivingClient.getQueueListeners();
//        List<QueueMessageReceiver> queueClosingListeners = receivingClosingClient.getQueueListeners();
//
//        log.info("Number of Subscriber [" + queueListeners.size() + "]");
//        log.info("Number of Closing Subscriber [" + queueClosingListeners.size() + "]");
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", queueNameArg, "100", "false",
//                                                            runTime.toString(), sendCount.toString(), noOfPublishers.toString(),
//                                                            "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        AndesClientUtilsTemp.waitUntilAllMessagesReceived(receivingClient, "MillionTenPercentSubscriberCloseQueue",
//                                                          noOfMessagesToExpect,
//                                                          runTime);
//
//        AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        AndesClientUtilsTemp.waitUntilExactNumberOfMessagesReceived(receivingClosingClient,
//                                                                    "MillionTenPercentSubscriberCloseQueue",
//                                                                    noOfMessagesToReceiveByClosingSubscribers, runTime);
//
//        Integer actualReceivedCount = receivingClient.getReceivedqueueMessagecount() + receivingClosingClient
//                .getReceivedqueueMessagecount();
//
//        log.info("Total Non Closing Subscribers Received Messages [" + receivingClient.getReceivedqueueMessagecount()
//                 + "]");
//        log.info("Total Closing Subscribers Received Messages [" + receivingClosingClient
//                .getReceivedqueueMessagecount() + "]");
//        log.info("Total Received Messages [" + actualReceivedCount + "]");
//
//        Assert.assertTrue(actualReceivedCount >= sendCount, "Received only " + actualReceivedCount + " messages.");
    }
}
