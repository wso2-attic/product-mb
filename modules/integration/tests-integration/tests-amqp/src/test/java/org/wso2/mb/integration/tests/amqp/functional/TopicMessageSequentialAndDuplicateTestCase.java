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
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.AndesJMSConsumerClient;
import org.wso2.mb.integration.common.clients.AndesJMSPublisherClient;
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
 * 1. Send messages to a single topic and receive them
 * 2. listen for some more time to see if there are duplicates coming
 * 3. check if messages were received in order
 * 4. check if messages have any duplicates
 */
public class TopicMessageSequentialAndDuplicateTestCase extends MBIntegrationBaseTest {


    private static final long SEND_COUNT = 1000L;

    @BeforeClass
    public void prepare() {
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "topic"})
    public void performTopicMessageSequentialAndDuplicateTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {



        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "singleTopic");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(SEND_COUNT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(SEND_COUNT/10L);


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "singleTopic");
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT/10L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);


        AndesJMSConsumerClient consumerClient = new AndesJMSConsumerClient(consumerConfig);
        consumerClient.startClient();

        AndesJMSPublisherClient publisherClient = new AndesJMSPublisherClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(consumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), SEND_COUNT, "Message receiving failed.");

        Assert.assertTrue(AndesClientUtils.checkIfMessagesAreInOrder(consumerClient), "Messages are not in order.");

        Assert.assertEquals(AndesClientUtils.checkIfMessagesAreDuplicated(consumerClient).keySet().size(), 0, "Duplicate messages received.");




//        Integer sendCount = 1000;
//        Integer runTime = 20;
//        Integer expectedCount = 5000;
//
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:singleTopic",
//                "100", "true", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:singleTopic", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean success = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean senderSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(senderSuccess, "Message sending failed.");
//        Assert.assertFalse(success, "Message receiving failed.");
//
//        Assert.assertEquals(receivingClient.getReceivedTopicMessagecount(), sendCount.intValue(), "Did not receive expected message count.");
//
//        Assert.assertTrue(receivingClient.checkIfMessagesAreInOrder(), "Messages are not in order.");
//
//        Assert.assertEquals(receivingClient.checkIfMessagesAreDuplicated().keySet().size(), 0, "Duplicate messages received.");
    }
}
