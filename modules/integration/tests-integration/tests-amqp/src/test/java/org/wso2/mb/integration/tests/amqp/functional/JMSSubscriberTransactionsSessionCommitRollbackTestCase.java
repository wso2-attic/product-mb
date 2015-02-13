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
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * 1. start a queue receiver with trasacted sessions
 * 2. send 10 messages
 * 3. after 10 messages are received rollback session
 * 4. do same 5 times.After 50 messages received commit the session and close subscriber
 * 5. analyse and see if each message is duplicated five times
 * 6. do another subscription to verify no more messages are received
 */
public class JMSSubscriberTransactionsSessionCommitRollbackTestCase extends MBIntegrationBaseTest {


    private static final long SEND_COUNT = 10L;
    private static final int ROLLBACK_ITERATIONS = 5;
    private static final long EXPECTED_COUNT = SEND_COUNT * ROLLBACK_ITERATIONS;

    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue", "transactions"})
    public void performJMSSubscriberTransactionsSessionCommitRollbackTestCase()
            throws AndesClientException, JMSException, NamingException, IOException,
                   CloneNotSupportedException {


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "transactionQueue");
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.SESSION_TRANSACTED);
        consumerConfig.setCommitAfterEachMessageCount(EXPECTED_COUNT);
        consumerConfig.setRollbackAfterEachMessageCount(SEND_COUNT);
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setFilePathToWriteReceivedMessages(System.getProperty("framework.resource.location") + File.separator + "receivedMessages.txt");
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(5L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "transactionQueue");
        publisherConfig.setPrintsPerMessageCount(150L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        // Creating clients
        AndesJMSConsumerClient initialConsumerClient = new AndesJMSConsumerClient(consumerConfig);
        initialConsumerClient.startClient();

        AndesJMSPublisherClient publisherClient = new AndesJMSPublisherClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(initialConsumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        AndesClientUtils.sleepForInterval(1000);

        Map<Long, Integer> duplicateMessages = AndesClientUtils.checkIfMessagesAreDuplicated(initialConsumerClient);

        boolean expectedCountDelivered = false;
        if (duplicateMessages != null) {
            for (Long messageIdentifier : duplicateMessages.keySet()) {
                int numberOfTimesDelivered = duplicateMessages.get(messageIdentifier);
                if (ROLLBACK_ITERATIONS == numberOfTimesDelivered) {
                    expectedCountDelivered = true;
                } else {
                    expectedCountDelivered = false;
                    break;
                }
            }
        }

        //verify no more messages are delivered

        AndesClientUtils.sleepForInterval(2000);

        AndesJMSConsumerClient secondaryConsumerClient = new AndesJMSConsumerClient(consumerConfig.clone());
        secondaryConsumerClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(initialConsumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed.");
        Assert.assertEquals(initialConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed.");
        Assert.assertTrue(expectedCountDelivered, "Expected message count was not delivered.");
        Assert.assertNotEquals(secondaryConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Messages received after the test.");









//        Integer sendCount = 10;
//        Integer runTime = 40;
//        int numberOfRollbackIterations = 5;
//        Integer expectedCount = sendCount * numberOfRollbackIterations;
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:transactionQueue",
//                "10", "true", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=0,delayBetweenMsg=0,rollbackAfterEach=" + sendCount + "," +
//                "commitAfterEach=" + expectedCount + ",stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:transactionQueue", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        AndesClientUtils.sleepForInterval(1000);
//
//        Map<Long, Integer> duplicateMessages = receivingClient.checkIfMessagesAreDuplicated();
//
//        boolean expectedCountDelivered = false;
//        if (duplicateMessages != null) {
//            for (Long messaggeIdentifier : duplicateMessages.keySet()) {
//                int numberOfTimesDelivered = duplicateMessages.get(messaggeIdentifier);
//                if (numberOfRollbackIterations == numberOfTimesDelivered) {
//                    expectedCountDelivered = true;
//                } else {
//                    expectedCountDelivered = false;
//                    break;
//                }
//            }
//        }
//
//        //verify no more messages are delivered
//
//        AndesClientUtils.sleepForInterval(2000);
//
//        receivingClient.startWorking();
//
//        boolean areMessagesReceivedAfterwars = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient,
//                expectedCount, 20);
//
//        Assert.assertEquals(sendSuccess && receiveSuccess && expectedCountDelivered && !areMessagesReceivedAfterwars, true);
//
//        Assert.assertTrue(sendSuccess, "Message sending failed.");
//        Assert.assertTrue(receiveSuccess, "Message receiving failed.");
//        Assert.assertTrue(expectedCountDelivered, "Expected message count was not delivered.");
//        Assert.assertFalse(areMessagesReceivedAfterwars, "Messages received after the test.");
    }

}
