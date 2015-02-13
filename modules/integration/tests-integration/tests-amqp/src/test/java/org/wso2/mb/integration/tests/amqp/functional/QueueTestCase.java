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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
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

public class QueueTestCase extends MBIntegrationBaseTest {
    private static final Log log = LogFactory.getLog(QueueTestCase.class);
    private static final long EXPECTED_COUNT_1000 = 1000L;
    private static final long SEND_COUNT_1000 = 1000L;
    private static final long EXPECTED_COUNT_3000 = 3000L;
    private static final long SEND_COUNT_3000 = 3000L;

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = "wso2.mb", description = "Single queue send-receive test case")
    public void performSingleQueueSendReceiveTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "singleQueue");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT_1000);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(100L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "singleQueue");
        publisherConfig.setPrintsPerMessageCount(100L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT_1000);

        AndesJMSConsumerClient consumerClient = new AndesJMSConsumerClient(consumerConfig);
        consumerClient.startClient();

        AndesJMSPublisherClient publisherClient = new AndesJMSPublisherClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(consumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT_1000, "Message sending failed");

        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT_1000, "Message receiving failed.");

//        Integer sendCount = 1000;
//        Integer runTime = 20;
//        Integer expectedCount = 1000;
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:singleQueue",
//                                                              "100", "false", runTime.toString(), expectedCount.toString(),
//                                                              "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
//                                                            runTime.toString(), sendCount.toString(), "1",
//                                                            "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(sendSuccess, "Message sending failed.");
//        Assert.assertTrue(receiveSuccess, "Message receiving failed.");
    }

    // Disabled until topic workflow is implemented
    @Test(groups = "wso2.mb", description = "subscribe to a topic and send message to a queue which has the same name" +
                                            " as queue", enabled = false)
    public void performSubTopicPubQueueTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "topic1");
        // Use a listener
        consumerConfig.setAsync(true);
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT_1000);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(100L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "topic1");
        publisherConfig.setPrintsPerMessageCount(100L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT_1000);

        AndesJMSConsumerClient consumerClient = new AndesJMSConsumerClient(consumerConfig);
        consumerClient.startClient();

        AndesJMSPublisherClient publisherClient = new AndesJMSPublisherClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(consumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT_1000, "Message sending failed");

        Assert.assertNotEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT_1000, "Messages should have not received");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), 0, "Messages should have not received");



//        Integer sendCount = 1000;
//        Integer runTime = 20;
//        Integer expectedCount = 1000;
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:topic1",
//                                                              "100", "false", runTime.toString(), expectedCount.toString(),
//                                                              "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:topic1", "100", "false",
//                                                            runTime.toString(), sendCount.toString(), "1",
//                                                            "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(sendSuccess, "Message sending failed.");
//        Assert.assertFalse(receiveSuccess, "Message sending failed.");
    }


    @Test(groups = "wso2.mb", description = "send large number of messages to a queue which has two consumers")
    public void performManyConsumersTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig1 = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "queue1");
        // Use a listener
        consumerConfig1.setAsync(true);
        // Amount of message to receive
        consumerConfig1.setMaximumMessagesToReceived(EXPECTED_COUNT_3000);
        // Prints per message
        consumerConfig1.setPrintsPerMessageCount(100L);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig2 = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "queue1");
        // Use a listener
        consumerConfig2.setAsync(true);
        // Amount of message to receive
        consumerConfig2.setMaximumMessagesToReceived(EXPECTED_COUNT_3000);
        // Prints per message
        consumerConfig2.setPrintsPerMessageCount(100L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "queue1");
        publisherConfig.setPrintsPerMessageCount(100L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT_3000);

        AndesJMSConsumerClient consumerClient1 = new AndesJMSConsumerClient(consumerConfig1);
        consumerClient1.startClient();

        AndesJMSConsumerClient consumerClient2 = new AndesJMSConsumerClient(consumerConfig2);
        consumerClient2.startClient();

        AndesJMSPublisherClient publisherClient = new AndesJMSPublisherClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.sleepForInterval(5000);

        long msgCountFromClient1 = consumerClient1.getReceivedMessageCount();
        long msgCountFromClient2 = consumerClient2.getReceivedMessageCount();

        Assert.assertEquals(msgCountFromClient1 + msgCountFromClient2, EXPECTED_COUNT_3000,
                            "Did not received expected message count");

//
//
//
//        Integer sendCount = 3000;
//        Integer runTime = 20;
//        Integer expectedCount = 3000;
//
//        AndesClientTemp receivingClient1 = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:queue1",
//                                                               "100", "false", runTime.toString(), expectedCount.toString(),
//                                                               "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient1.startWorking();
//        AndesClientTemp receivingClient2 = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:queue1",
//                                                               "100", "false", runTime.toString(), expectedCount.toString(),
//                                                               "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//        receivingClient2.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:queue1", "100", "false",
//                                                            runTime.toString(), sendCount.toString(), "1",
//                                                            "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//        int msgCountFromClient1 = AndesClientUtilsTemp.getNoOfMessagesReceived(receivingClient1, expectedCount, runTime);
//        int msgCountFromClient2 = AndesClientUtilsTemp.getNoOfMessagesReceived(receivingClient2, expectedCount, runTime);
//
//        assertEquals(msgCountFromClient1 + msgCountFromClient2, expectedCount.intValue(),
//                     "Did not received expected message count");
    }

}
