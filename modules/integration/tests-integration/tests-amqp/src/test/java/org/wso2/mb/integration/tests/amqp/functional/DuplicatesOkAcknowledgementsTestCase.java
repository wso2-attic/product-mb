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
import org.wso2.mb.integration.common.clients.AndesJMSConsumerClient;
import org.wso2.mb.integration.common.clients.AndesJMSPublisherClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;

/**
 * This class includes test cases to test duplicate acknowledgements modes for queues
 */
public class DuplicatesOkAcknowledgementsTestCase extends MBIntegrationBaseTest {

    private static final long SEND_COUNT = 100L;
    private static final long EXPECTED_COUNT = 100L;

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
     * 2. Receive messages acking message bunch to bunch
     * 3. Check whether all messages received
     */
    @Test(groups = "wso2.mb", description = "Single queue send-receive test case with dup messages")
    public void duplicatesOkAcknowledgementsTest()
            throws AndesClientException, JMSException, NamingException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "dupOkAckTestQueue");
        // Use a listener
        consumerConfig.setAsync(true);
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.DUPS_OK_ACKNOWLEDGE);
        consumerConfig.setPrintsPerMessageCount(10L);


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "dupOkAckTestQueue");
        publisherConfig.setPrintsPerMessageCount(10L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);


        AndesJMSConsumerClient consumerClient = new AndesJMSConsumerClient(consumerConfig);
        consumerClient.startClient();

        AndesJMSPublisherClient publisherClient = new AndesJMSPublisherClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.sleepForInterval(5000);

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(consumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);


        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message send failed");
        Assert.assertTrue(consumerClient.getReceivedMessageCount() >= EXPECTED_COUNT, "The number of received messages should be equal or more than the amount sent");


//        //Stop receiving client
//
//        //Get duplicates
//        long duplicateCount = AndesClientUtils.getTotalNumberOfDuplicates();
//
//
//        Integer sendCount = 100;
//        Integer runTime = 20;
//        Integer expectedCount = 100;
//        Integer duplicateCount;
////        //Create receiving client
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:dupOkAckTestQueue",
//                                                              "100", "false", runTime.toString(), expectedCount.toString(),
//                                                              "1", "listener=true,ackMode=3,delayBetweenMsg=10,stopAfter=500",
//                                                              "");
//        //Start receiving client
//        receivingClient.startWorking();
//
//        //Create sending client
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:dupOkAckTestQueue", "100", "false",
//                                                    runTime.toString(), sendCount.toString(), "1",
//                                                    "ackMode=1,delayBetweenMsg=10,stopAfter=" + sendCount, "");
//        //Start sending client
//        sendingClient.startWorking();
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, 3 * expectedCount, runTime);
//        Integer totalMessagesReceived = receivingClient.getReceivedqueueMessagecount();
//        log.info("RECEIVED MESSAGES : " +  totalMessagesReceived.toString());
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//        //Stop receiving client
//        receivingClient.shutDownClient();
//        //Get duplicates
//        duplicateCount = receivingClient.getTotalNumberOfDuplicates();
//        Assert.assertTrue(sendSuccess, "Messaging sending failed");
//        Assert.assertEquals(totalMessagesReceived, (Integer) (expectedCount + duplicateCount),
//                            "Total number of received message should be equal sum of expected and duplicate message count ");
    }
}
