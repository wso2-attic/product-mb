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


/**
 * 1. use two queues q1, q2. 2 subscribers for q1 and one subscriber for q2
 * 2. use two publishers for q1 and one for q2
 * 3. check if messages were received correctly
 */
public class MultipleQueueSendReceiveTestCase extends MBIntegrationBaseTest {

    private static final long SEND_COUNT = 2000L;
    private static final long ADDITIONAL_COUNT = 10L;
    private static final long EXPECTED_COUNT = SEND_COUNT + ADDITIONAL_COUNT;

    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performMultipleQueueSendReceiveTestCase()
            throws AndesClientException, CloneNotSupportedException, JMSException, NamingException,
                   IOException {


// Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig1 = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "multipleQueue1");
        // Amount of message to receive
        consumerConfig1.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig1.setPrintsPerMessageCount(100L);
        consumerConfig1.setSubscriberCount(3);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig2 = consumerConfig1.clone();
        consumerConfig2.setDestinationName("multipleQueue2");


        AndesJMSPublisherClientConfiguration publisherConfig1 = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "multipleQueue1");
        publisherConfig1.setPrintsPerMessageCount(100L);
        publisherConfig1.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig1.setPublisherCount(3);

        AndesJMSPublisherClientConfiguration publisherConfig2 = publisherConfig1.clone();
        publisherConfig2.setDestinationName("multipleQueue2");

        AndesJMSConsumerClient consumerClient1 = new AndesJMSConsumerClient(consumerConfig1);
        consumerClient1.startClient();

        AndesJMSConsumerClient consumerClient2 = new AndesJMSConsumerClient(consumerConfig2);
        consumerClient2.startClient();

        AndesJMSPublisherClient publisherClient1 = new AndesJMSPublisherClient(publisherConfig1);
        publisherClient1.startClient();

        AndesJMSPublisherClient publisherClient2 = new AndesJMSPublisherClient(publisherConfig2);
        publisherClient2.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(consumerClient1,  AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(consumerClient2,  AndesClientConstants.DEFAULT_RUN_TIME);

        long receivedMessageCount = consumerClient1.getReceivedMessageCount() + consumerClient2.getReceivedMessageCount();
        long messageCountRequired = EXPECTED_COUNT - ADDITIONAL_COUNT;

        Assert.assertEquals(receivedMessageCount, messageCountRequired, "Expected message count was not received.");


//        Integer sendCount = 2000;
//        Integer runTime = 20;
//        int additional = 10;
//
//        //wait some more time to see if more messages are received
//        Integer expectedCount = 2000 + additional;
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:multipleQueue1," +
//                "multipleQueue2,", "100", "false",
//                runTime.toString(), EXPECTED_COUNT.toString(), "3",
//                "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:multipleQueue1,multipleQueue2",
//                "100",
//                "false", runTime.toString(), sendCount.toString(), "3",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, EXPECTED_COUNT, runTime);
//
//        Integer receivedMessageCount = receivingClient.getReceivedqueueMessagecount();
//        Integer messageCountRequired = EXPECTED_COUNT - additional;
//
//        Assert.assertEquals(receivedMessageCount, messageCountRequired, "Expected message count was not received.");
    }
}
