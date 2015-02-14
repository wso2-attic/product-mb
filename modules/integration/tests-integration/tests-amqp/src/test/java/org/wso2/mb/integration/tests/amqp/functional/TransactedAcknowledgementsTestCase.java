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
import org.wso2.mb.integration.common.clients.AndesClient;
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
 * This class includes test cases to test transacted acknowledgements modes for queues
 */
public class TransactedAcknowledgementsTestCase extends MBIntegrationBaseTest {

    private static final long EXPECTED_COUNT = 10L;
    private static final long SEND_COUNT = 10L;

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
     * 1. Start a queue receiver with transacted sessions
     * 2. Send 10 messages
     * 3. After 10 messages are received rollback session
     * 4. After 50 messages received commit the session and close subscriber
     * 5. Analyse and see if each message is duplicated five times
     */
    @Test(groups = "wso2.mb", description = "Single queue send-receive test case with transactions")
    public void transactedAcknowledgements()
            throws AndesClientException, JMSException, NamingException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig1 = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "transactedAckTestQueue");
        // Use a listener
        consumerConfig1.setAsync(true);
        // Amount of message to receive
        consumerConfig1.setMaximumMessagesToReceived(100L);
        consumerConfig1.setRunningDelay(100L);
        consumerConfig1.setRollbackAfterEachMessageCount(10);
        consumerConfig1.setCommitAfterEachMessageCount(30);
        consumerConfig1.setAcknowledgeMode(JMSAcknowledgeMode.SESSION_TRANSACTED);


        AndesJMSPublisherClientConfiguration publisherConfig1 = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "transactedAckTestQueue");
        publisherConfig1.setNumberOfMessagesToSend(SEND_COUNT);



        AndesClient consumerClient = new AndesClient(consumerConfig1);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig1);
        publisherClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(consumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Messaging sending failed");

        //If received messages less than expected number wait until received again
        //Get rollback status , check message id of next message of roll backed message equal to first message
        long duplicateCount = consumerClient.getTotalNumberOfDuplicates();
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), (EXPECTED_COUNT + duplicateCount), "Total number of received message should be equal sum of expected and duplicate message count ");
        Assert.assertTrue(consumerClient.transactedOperation(10L), "After rollback next message need to equal first message of batch");

//        Integer sendCount = 10;
//        Integer runTime = 20;
//        int expectedCount = 10;
//        //Create receiving client
//        AndesClientTemp receivingClient =
//                new AndesClientTemp("receive", "127.0.0.1:5672", "queue:transactedAckTestQueue", "100", "true",
//                                runTime.toString(), String.valueOf(expectedCount), "1",
//                                "listener=true,ackMode=0,delayBetweenMsg=100,stopAfter=100,rollbackAfterEach=10,commitAfterEach=30",
//                                "");
//        //Start receiving client
//        receivingClient.startWorking();
//        //Create sending client
//        AndesClientTemp sendingClient =
//                new AndesClientTemp("send", "127.0.0.1:5672", "queue:transactedAckTestQueue", "100", "false",
//                                runTime.toString(), sendCount.toString(), "1",
//                                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//        //Start sending client
//        sendingClient.startWorking();
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//        int totalMessagesReceived = receivingClient.getReceivedqueueMessagecount();
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//        //If received messages less than expected number wait until received again
//        //Get rollback status , check message id of next message of roll backed message equal to first message
//        int duplicateCount = receivingClient.getTotalNumberOfDuplicates();
//        Assert.assertTrue(sendSuccess, "Messaging sending failed");
//        Assert.assertEquals(totalMessagesReceived, (expectedCount + duplicateCount),
//                            "Total number of received message should be equal sum of expected and duplicate message count ");
//        Assert.assertTrue(receivingClient.transactedOperation(10),
//                          "After rollback next message need to equal first message of batch");
    }
}
