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
 * 1. use two topics t1, t2. 2 subscribers for t1 and one subscriber for t2
 * 2. use two publishers for t1 and one for t2
 * 3. check if messages were received correctly
 */
public class MultipleTopicPublishSubscribeTestCase {
    private static final long SEND_COUNT_1000 = 1000L;
    private static final long SEND_COUNT_2000 = 2000L;
    private static final long ADDITIONAL = 10L;

    //expect little more to check if no more messages are received
    private static final long EXPECTED_COUNT_4010 = 4000L + ADDITIONAL;
    private static final long EXPECTED_COUNT_1010 = 1000L + ADDITIONAL;

    @BeforeClass
    public void prepare() {
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "topic"})
    public void performMultipleTopicPublishSubscribeTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig1 = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "multipleTopic2");
        // Amount of message to receive
        consumerConfig1.setMaximumMessagesToReceived(EXPECTED_COUNT_1010);
        // Prints per message
        consumerConfig1.setPrintsPerMessageCount(EXPECTED_COUNT_1010 / 10);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig2 = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "multipleTopic1");
        // Amount of message to receive
        consumerConfig2.setMaximumMessagesToReceived(EXPECTED_COUNT_4010);
        // Prints per message
        consumerConfig2.setPrintsPerMessageCount(EXPECTED_COUNT_4010 / 10);

        AndesJMSPublisherClientConfiguration publisherConfig1 = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "multipleTopic2");
        publisherConfig1.setPrintsPerMessageCount(100L);
        publisherConfig1.setNumberOfMessagesToSend(SEND_COUNT_2000);

        AndesJMSPublisherClientConfiguration publisherConfig2 = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "multipleTopic1");
        publisherConfig2.setPrintsPerMessageCount(100L);
        publisherConfig2.setNumberOfMessagesToSend(SEND_COUNT_1000);

        AndesClient consumerClient1 = new AndesClient(consumerConfig1, 2);
        consumerClient1.startClient();

        AndesClient consumerClient2 = new AndesClient(consumerConfig2);
        consumerClient2.startClient();

        AndesClient publisherClient1 = new AndesClient(publisherConfig1, 2);
        publisherClient1.startClient();

        AndesClient publisherClient2 = new AndesClient(publisherConfig2);
        publisherClient2.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient1.getSentMessageCount(), SEND_COUNT_2000, "Publisher publisherClient1 failed to publish messages");
        Assert.assertEquals(publisherClient2.getSentMessageCount(), SEND_COUNT_1000, "Publisher publisherClient2 failed to publish messages");
        Assert.assertEquals(consumerClient1.getReceivedMessageCount(), EXPECTED_COUNT_4010 - ADDITIONAL,
                            "Did not receive expected message count for multipleTopic1.");
        Assert.assertEquals(consumerClient2.getReceivedMessageCount(), EXPECTED_COUNT_1010 - ADDITIONAL,
                            "Did not receive expected message count for multipleTopic2.");


//        Integer sendCount1 = 1000;
//        Integer sendCount2 = 2000;
//        Integer runTime = 40;
//        int additional = 10;
//
//        //expect little more to check if no more messages are received
//        Integer expectedCount2 = 4000 + additional;
//        Integer expectedCount1 = 1000 + additional;
//
//        AndesClientTemp receivingClient2 = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:multipleTopic2,", "100",
//                "false",
//                runTime.toString(), expectedCount2.toString(), "2",
//                "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount2, "");
//
//        AndesClientTemp receivingClient1 = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:multipleTopic1,", "100",
//                "false",
//                runTime.toString(), expectedCount1.toString(), "1",
//                "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount1, "");
//
//        receivingClient1.startWorking();
//        receivingClient2.startWorking();
//
//
//        AndesClientTemp sendingClient2 = new AndesClientTemp("send", "127.0.0.1:5672", "topic:multipleTopic2", "100",
//                "false", runTime.toString(), sendCount2.toString(), "2",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount2, "");
//
//        AndesClientTemp sendingClient1 = new AndesClientTemp("send", "127.0.0.1:5672", "topic:multipleTopic1", "100",
//                "false", runTime.toString(), sendCount1.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount1, "");
//
//        sendingClient1.startWorking();
//        sendingClient2.startWorking();
//
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient1, expectedCount1, runTime);
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient2, expectedCount2, runTime);
//
//        Assert.assertEquals(receivingClient1.getReceivedTopicMessagecount(), expectedCount1 - additional,
//                "Did not receive expected message count for multipleTopic1.");
//        Assert.assertEquals(receivingClient2.getReceivedTopicMessagecount(), expectedCount2 - additional,
//                "Did not receive expected message count for multipleTopic2.");
    }
}
