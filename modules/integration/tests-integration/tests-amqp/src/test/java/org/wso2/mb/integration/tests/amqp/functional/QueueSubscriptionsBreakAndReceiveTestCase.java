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

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;


/**
 * 1. subscribe to a single queue which will take 1/5 messages of sent and stop
 * 2. send messages to the queue
 * 3. close and resubscribe 5 times to the queue
 * 4. verify message count is equal to the sent total
 */
public class QueueSubscriptionsBreakAndReceiveTestCase extends MBIntegrationBaseTest {
    private static Logger log = Logger.getLogger(QueueSubscriptionsBreakAndReceiveTestCase.class);
    private static final long SEND_COUNT = 1000L;
    private static final int NUMBER_OF_SUBSCRIPTION_BREAKS = 5;
    private static final long EXPECTED_COUNT = SEND_COUNT / NUMBER_OF_SUBSCRIPTION_BREAKS;


    @BeforeClass
    public void prepare() {
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performQueueSubscriptionsBreakAndReceiveTestCase()
            throws AndesClientException, NamingException, JMSException, IOException {


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "breakSubscriberQueue");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "breakSubscriberQueue");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed");

        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed for initial consumer.");

        long totalMessageCountReceived = consumerClient.getReceivedMessageCount();

        //anyway wait one more iteration to verify no more messages are delivered
        for (int count = 1; count < NUMBER_OF_SUBSCRIPTION_BREAKS; count++) {

            AndesClient newConsumerClient = new AndesClient(consumerConfig);
            newConsumerClient.startClient();
//            long waitingFactor = 4L;
//            log.info("Waiting for " + (AndesClientConstants.DEFAULT_RUN_TIME * waitingFactor) / 1000 + " seconds to see updates on received message counter.");
            AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(newConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME * 2L);
            totalMessageCountReceived = totalMessageCountReceived + newConsumerClient.getReceivedMessageCount();
//            AndesClientUtils.sleepForInterval(1000L);
//            Assert.assertEquals(newConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed.");
        }

        Assert.assertEquals(totalMessageCountReceived, SEND_COUNT, "Expected message count was not received.");

//        Integer sendCount = 1000;
//        Integer runTime = 30;
//        int numberOfSubscriptionBreaks = 5;
//        Integer expectedCount = sendCount / numberOfSubscriptionBreaks;
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:breakSubscriberQueue",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:breakSubscriberQueue", "100",
//                "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean success = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        Assert.assertTrue(success, "Message receiving failed.");
//
//        int totalMsgCountReceived = receivingClient.getReceivedqueueMessagecount();
//
//        //anyway wait one more iteration to verify no more messages are delivered
//        for (int count = 1; count < numberOfSubscriptionBreaks; count++) {
//
//            receivingClient.startWorking();
//            AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//            totalMsgCountReceived += receivingClient.getReceivedqueueMessagecount();
//            AndesClientUtils.sleepForInterval(1000);
//        }
//
//        Assert.assertEquals(totalMsgCountReceived, sendCount.intValue(), "Expected message count was not received.");
    }

}
