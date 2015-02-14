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
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;

/**
 * 1. start a queue receiver in client ack mode
 * 2. receive messages acking message bunch to bunch
 * 3. after all messages are received subscribe again and verify n more messages come
 */
public class ClientAcknowledgementsTestCase extends MBIntegrationBaseTest {

    private static final long EXPECTED_COUNT = 1000L;
    private static final long SEND_COUNT = 1000L;

    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performClientAcknowledgementsTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "clientAckTestQueue");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(100L);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE);
        consumerConfig.setAcknowledgeAfterEachMessageCount(200L);


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "clientAckTestQueue");
        publisherConfig.setPrintsPerMessageCount(100L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);


        AndesClient consumerClient1 = new AndesClient(consumerConfig);
        consumerClient1.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(consumerClient1,  AndesClientConstants.DEFAULT_RUN_TIME);

        AndesClient consumerClient2 = new AndesClient(consumerConfig);
        consumerClient2.startClient();

        AndesClientUtilsTemp.sleepForInterval(2000);

        long totalMessagesReceived = consumerClient1.getReceivedMessageCount() + consumerClient2.getReceivedMessageCount();

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Expected message count not received.");
        Assert.assertEquals(totalMessagesReceived, EXPECTED_COUNT, "Expected message count not received.");

//        Integer sendCount = 1000;
//        Integer runTime = 20;
//        Integer expectedCount = 1000;

//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:clientAckTestQueue",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=2,delayBetweenMsg=0,ackAfterEach=200,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:clientAckTestQueue", "100",
//                "false",
//                runTime.toString(), sendCount.toString(), "1", "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount,
//                "");
//
//        sendingClient.startWorking();
//
//        boolean success = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        Integer totalMsgsReceived = receivingClient.getReceivedqueueMessagecount();
//
//        AndesClientUtilsTemp.sleepForInterval(2000);
//
//        receivingClient.startWorking();
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, 15);
//
//        totalMsgsReceived += receivingClient.getReceivedqueueMessagecount();
//
//        Assert.assertTrue(success, "Message receiving failed.");
//
//        Assert.assertEquals(totalMsgsReceived, expectedCount, "Expected message count not received.");
    }

}
