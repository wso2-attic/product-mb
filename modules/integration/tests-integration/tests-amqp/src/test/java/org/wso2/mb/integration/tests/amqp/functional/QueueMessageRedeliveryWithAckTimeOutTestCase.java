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
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;

/**
 * 1. subscribe to a single queue using Client Ack
 * 2. this subscriber will wait a long time for messages (defaultAckWaitTimeout*defaultMaxRedeliveryAttempts)
 * 3. subscriber will never ack for messages
 * 4. subscriber will receive same message until defaultMaxRedeliveryAttempts breached
 * 5. after that message will be written to dlc
 * 6. no more message should be delivered after written to DLC
 */
public class QueueMessageRedeliveryWithAckTimeOutTestCase extends MBIntegrationBaseTest {

    private static final long DEFAULT_ACK_WAIT_TIMEOUT = 10L;
    private static final long SEND_COUNT = 1L;
    private static final long EXPECTED_COUNT = 10L;

    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performQueueMessageRedeliveryWithAckTimeOutTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

//wait until messages go to DLC and some more time to verify no more messages are coming


        // set AckwaitTimeout
        System.setProperty("AndesAckWaitTimeOut", Long.toString(DEFAULT_ACK_WAIT_TIMEOUT * 1000L));



        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "redeliveryQueue");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(5L);
        consumerConfig.setAcknowledgeAfterEachMessageCount(200L);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "redeliveryQueue");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);


        AndesClient consumerClient = new AndesClient(consumerConfig, 2);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();


        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME * 2L);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message send failed");

        Assert.assertNotEquals(consumerClient.getReceivedMessageCount(), 0, "Message receiving failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive expected message count");


//        int defaultMaxRedeliveryAttempts = 10;
//        int defaultAckWaitTimeout = 10;
//        Integer sendCount = 2;
//
//        //wait until messages go to DLC and some more time to verify no more messages are coming
//        Integer expectedCount = defaultMaxRedeliveryAttempts * sendCount;
//
//        //wait until messages go to DLC and some more time to verify no more messages are coming
//        Integer runTime = defaultAckWaitTimeout * defaultMaxRedeliveryAttempts + 200;
//
//        // set AckwaitTimeout
//        System.setProperty("AndesAckWaitTimeOut", Integer.toString(defaultAckWaitTimeout * 1000));
//        // expect 1000 messages to stop it from stopping
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:redeliveryQueue",
//                "100", "true", runTime.toString(), expectedCount.toString(),
//                "2", "listener=true,ackMode=2,ackAfterEach=200,delayBetweenMsg=0,stopAfter=" + expectedCount.toString(), "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:redeliveryQueue", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean success = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(sendSuccess, "Message sending failed.");
//        Assert.assertTrue(success, "Message receiving failed.");
//
//        Assert.assertEquals(receivingClient.getReceivedqueueMessagecount(), sendCount * defaultMaxRedeliveryAttempts,
//                "Did not receive expected message count");
    }
}
