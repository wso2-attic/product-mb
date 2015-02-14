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
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;

/**
 * 1. start a durable topic subscription
 * 2. send 1500 messages
 * 3. after 500 messages were received close the subscriber
 * 4. subscribe again. after 500 messages were received unsubscribe
 * 5. subscribe again. Verify no more messages are coming
 */
public class DurableTopicTestCase {

    private static final long SEND_COUNT = 1500L;
    private static final long EXPECTED_COUNT = 500L;

    @BeforeClass
    public void prepare() {
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "durableTopic"})
    public void performDurableTopicTestCase()
            throws AndesClientException, JMSException, NamingException, IOException,
                   CloneNotSupportedException {


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig1 = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "durableTopic");
        consumerConfig1.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig1.setPrintsPerMessageCount(50L);
        consumerConfig1.setDurable(true, "sub1");

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "durableTopic");
        publisherConfig.setPrintsPerMessageCount(150L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        // Creating clients
        AndesClient initialConsumerClient = new AndesClient(consumerConfig1);
        initialConsumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        //Wait until messages receive
        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(initialConsumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig2 = consumerConfig1.clone();
        consumerConfig2.setUnSubscribeAfterEachMessageCount(EXPECTED_COUNT);

        // Creating clients
        AndesClient secondaryConsumerClient = new AndesClient(consumerConfig2);
        secondaryConsumerClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(secondaryConsumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig3 = consumerConfig2.clone();

        // Creating clients
        AndesClient tertiaryConsumerClient = new AndesClient(consumerConfig3);
        tertiaryConsumerClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(secondaryConsumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        AndesClientUtils.sleepForInterval(5000);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed.");

        // TODO : issue with the earlier implementation
        Assert.assertEquals(initialConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed for client 1.");
        Assert.assertEquals(secondaryConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed for client 2.");
        Assert.assertEquals(tertiaryConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed for client 3.");








//        Integer sendCount = 1500;
//        Integer runTime = 20;
//        Integer expectedCount = 500;
//
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:durableTopic",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0," +
//                "stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receivingSuccess1 = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount,
//                                                                                      runTime);
//
//        boolean sendingSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        //we just closed the subscription. Rest of messages should be delivered now.
//
//        AndesClientUtils.sleepForInterval(2000);
//
//        AndesClientTemp receivingClient2 = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:durableTopic",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0," +
//                "unsubscribeAfter=" + expectedCount + ",stopAfter=" + expectedCount, "");
//        receivingClient2.startWorking();
//
//        boolean receivingSuccess2 = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient2, expectedCount,
//                runTime);
//
//
//        //now we have unsubscribed the topic subscriber no more messages should be received
//
//
//        AndesClientTemp receivingClient3 = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:durableTopic",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0," +
//                "unsubscribeAfter=" + expectedCount + ",stopAfter=" + expectedCount, "");
//        receivingClient3.startWorking();
//
//        AndesClientUtils.sleepForInterval(5000);
//
//        boolean receivingSuccess3 = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient3, expectedCount,
//                runTime);
//
//        Assert.assertTrue(sendingSuccess, "Message sending failed.");
//
//        Assert.assertTrue(receivingSuccess1, "Message receiving failed for client 1.");
//
//        Assert.assertTrue(receivingSuccess2, "Message receiving failed for client 2.");
//
//        Assert.assertFalse(receivingSuccess3, "Message received from client 3 when no more messages should be received.");

    }
}
