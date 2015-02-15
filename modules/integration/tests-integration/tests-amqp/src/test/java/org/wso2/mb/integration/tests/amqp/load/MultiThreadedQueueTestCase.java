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

package org.wso2.mb.integration.tests.amqp.load;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
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
import java.util.ArrayList;
import java.util.List;

/**
 * send 10,000 messages collaberatively by 20 threads and receive collaberatively by 20 threads and
 * see if all messages are received
 */
public class MultiThreadedQueueTestCase extends MBIntegrationBaseTest {
    private static final long SEND_COUNT = 10000L;
    private static final long EXPECTED_COUNT = SEND_COUNT;
    private static final int NUMBER_OF_SUBSCRIBERS = 7;
    private static final int NUMBER_OF_PUBLISHERS = 7;

    @BeforeClass
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = "wso2.mb",
            description = "Multiple queue senders - multiple queue receivers test case")
    public void performMultiThreadedQueueTestCase()
            throws AndesClientException, NamingException, JMSException, IOException {
        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "multiThreadQueue");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT/10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "multiThreadQueue");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT/10L);

        AndesClient consumerClient = new AndesClient(consumerConfig, NUMBER_OF_SUBSCRIBERS);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, NUMBER_OF_PUBLISHERS);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT * NUMBER_OF_PUBLISHERS, "Message sending failed");

        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT * NUMBER_OF_SUBSCRIBERS, "Message receiving failed.");




//        Integer sendCount = 10000;
//        Integer numberOfPublisherThreads = 7;
//        Integer numberOfSubscriberThreads = 7;
//        Integer runTime = 200;
//        Integer expectedCount = 10000;
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672",
//                "queue:multiThreadQueue",
//                "100", "false", runTime.toString(),
//                expectedCount.toString(),
//                numberOfSubscriberThreads.toString(),
//                "listener=true,ackMode=1,delayBetweenMsg=0," +
//                        "stopAfter=" + expectedCount,
//                "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672",
//                "queue:multiThreadQueue",
//                "100", "false",
//                runTime.toString(), sendCount.toString(),
//                numberOfPublisherThreads.toString(),
//                "ackMode=1,delayBetweenMsg=0," +
//                        "stopAfter=" + sendCount,
//                "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(sendSuccess, "Message sending failed.");
//        Assert.assertTrue(receiveSuccess, "Message receiving failed.");
    }
}
