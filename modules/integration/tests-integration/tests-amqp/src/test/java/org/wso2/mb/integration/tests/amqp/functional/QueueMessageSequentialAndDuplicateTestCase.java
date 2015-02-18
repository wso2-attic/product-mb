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
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;

/**
 * 1. send 1000 messages and subscribe them for a single queue (set expected count to more than 1000 so it will wait)
 * 2. check if messages were received in order
 * 3. check if there are any duplicates
 */
public class QueueMessageSequentialAndDuplicateTestCase extends MBIntegrationBaseTest {

    private static final long EXPECTED_COUNT = 1000L;
    private static final long SEND_COUNT = 1000L;

    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performQueueMessageSequentialAndDuplicateTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "singleQueue");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT/10L);
        consumerConfig.setFilePathToWriteReceivedMessages(AndesClientConstants.FILE_PATH_TO_WRITE_RECEIVED_MESSAGES);


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "singleQueue");
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT/10L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);


        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message send failed");

        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed");

        Assert.assertTrue(consumerClient.checkIfMessagesAreInOrder(), "Messages are not in order");

        Assert.assertEquals(consumerClient.checkIfMessagesAreDuplicated().keySet().size(), 0, "Duplicate message are available.");





//        Integer sendCount = 1000;
//        Integer runTime = 20;
//        Integer expectedCount = 5000;
//
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:singleQueue",
//                "100", "true", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean senderSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(senderSuccess, "Message sending failed.");
//
//        Assert.assertEquals(receivingClient.getReceivedqueueMessagecount(), sendCount.intValue());
//
//        Assert.assertTrue(receivingClient.checkIfMessagesAreInOrder(), "Messages are not in order");
//
//        Assert.assertEquals(receivingClient.getDuplicatedMessages().keySet().size(), 0, "Duplicate message are available.");
    }
}
