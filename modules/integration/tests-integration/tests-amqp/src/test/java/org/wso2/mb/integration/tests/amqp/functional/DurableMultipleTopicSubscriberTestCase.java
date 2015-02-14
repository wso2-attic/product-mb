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

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;

/**
 * 1. start two durable topic subscription
 * 2. send 1500 messages
 */
public class DurableMultipleTopicSubscriberTestCase {

    private static final long EXPECTED_COUNT = 500L;
    private static final long SEND_COUNT = 1000L;

    @Test(groups = {"wso2.mb", "durableTopic"})
    public void performMultipleDurableTopicTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig1 = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "durableTopicMultiple");
        // Amount of message to receive
        consumerConfig1.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
//        consumerConfig1.setPrintsPerMessageCount(EXPECTED_COUNT/10L);
        consumerConfig1.setDurable(true, "sub1");

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig2 = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "durableTopicMultiple");
        // Amount of message to receive
        consumerConfig2.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
//        consumerConfig2.setPrintsPerMessageCount(EXPECTED_COUNT/10L);
        consumerConfig2.setDurable(true, "sub2");

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "durableTopicMultiple");
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT/10L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);


        AndesClient consumerClient1 = new AndesClient(consumerConfig1);
        consumerClient1.startClient();

        AndesClient consumerClient2 = new AndesClient(consumerConfig2);
        consumerClient2.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.sleepForInterval(4000);

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(consumerClient1.getReceivedMessageCount(), EXPECTED_COUNT, "Message receive error from sub1");

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(consumerClient2.getReceivedMessageCount(), EXPECTED_COUNT, "Message receive error from sub2");

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message send failed");


//        Integer sendCount = 1000;
//        Integer runTime = 20;
//        Integer expectedCount = 500;
//
//        // Start subscription 1
//        AndesClientTemp receivingClient1 = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:durableTopic",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0," +
//                "stopAfter=" + expectedCount, "");
//        receivingClient1.startWorking();
//
//        // Start subscription 2
//        AndesClientTemp receivingClient2 = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:durableTopic",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub2,delayBetweenMsg=0," +
//                "stopAfter=" + expectedCount, "");
//        receivingClient2.startWorking();
//
//        // Start message publisher
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//        sendingClient.startWorking();
//
//        AndesClientUtils.sleepForInterval(2000);
//
//        boolean receivingSuccess1 = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient1, expectedCount,
//                                                                                      runTime);
//        Assert.assertTrue(receivingSuccess1, "Message receive error from subscriber 1");
//
//        boolean receivingSuccess2 = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient2, expectedCount,
//                                                                                      runTime);
//        Assert.assertTrue(receivingSuccess2, "Message receive error from subscriber 2");
//
//        AndesClientUtils.sleepForInterval(2000);
//
//        boolean sendingSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//        Assert.assertTrue(sendingSuccess, "Message send error");
    }
}
