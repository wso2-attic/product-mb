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

public class TopicTestCase extends MBIntegrationBaseTest {

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = "wso2.mb", description = "Single topic send-receive test case")
    public void performSingleTopicSendReceiveTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {
        long sendCount = 1000L;
        long expectedCount = 1000L;

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "singleTopic");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(100L);


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "singleTopic");
        publisherConfig.setPrintsPerMessageCount(100L);
        publisherConfig.setNumberOfMessagesToSend(sendCount);


        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(consumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message send failed");

        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "TENANT 1 receive failed");




//        Integer sendCount = 1000;
//        Integer runTime = 20;
//        Integer expectedCount = 1000;
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:singleTopic",
//                "100", "false", runTime.toString(), expectedCount.toString(),
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
//        boolean receiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(sendSuccess, "Message sending failed.");
//        Assert.assertTrue(receiveSuccess, "Message receiving failed.");
    }

    @Test(groups = "wso2.mb", description = "")
    public void performMultipleTenantTopicSendReceiveTestCase()
            throws AndesClientException, CloneNotSupportedException, JMSException, NamingException,
                   IOException {
        long sendCount = 100L;
        long expectedCount = 100L;

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration adminConsumerConfig = new AndesJMSConsumerClientConfiguration("admin", "admin", "127.0.0.1", 5672, ExchangeType.TOPIC, "commontopic");
        // Use a listener
        adminConsumerConfig.setAsync(true);
        // Amount of message to receive
        adminConsumerConfig.setMaximumMessagesToReceived(expectedCount);
        // Prints per message
        adminConsumerConfig.setPrintsPerMessageCount(100L);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration tenant1ConsumerConfig = new AndesJMSConsumerClientConfiguration("tenant1user1!testtenant1.com", "tenant1user1", "127.0.0.1", 5672, ExchangeType.TOPIC, "testtenant1.com/commontopic");
        // Use a listener
        tenant1ConsumerConfig.setAsync(true);
        // Amount of message to receive
        tenant1ConsumerConfig.setMaximumMessagesToReceived(expectedCount);
        // Prints per message
        tenant1ConsumerConfig.setPrintsPerMessageCount(100L);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration tenant2ConsumerConfig = new AndesJMSConsumerClientConfiguration("tenant2user1!testtenant2.com", "tenant2user1", "127.0.0.1", 5672, ExchangeType.TOPIC, "testtenant2.com/commontopic");
        // Use a listener
        tenant2ConsumerConfig.setAsync(true);
        // Amount of message to receive
        tenant2ConsumerConfig.setMaximumMessagesToReceived(expectedCount);
        // Prints per message
        tenant2ConsumerConfig.setPrintsPerMessageCount(100L);


        AndesJMSPublisherClientConfiguration adminPublisherConfig = new AndesJMSPublisherClientConfiguration("admin", "admin", "127.0.0.1", 5672, ExchangeType.TOPIC, "commontopic");
        adminPublisherConfig.setPrintsPerMessageCount(100L);
        adminPublisherConfig.setNumberOfMessagesToSend(sendCount);

        AndesJMSPublisherClientConfiguration tenant1PublisherConfig = new AndesJMSPublisherClientConfiguration("tenant1user1!testtenant1.com", "tenant1user1", "127.0.0.1", 5672, ExchangeType.TOPIC, "testtenant1.com/commontopic");
        tenant1PublisherConfig.setPrintsPerMessageCount(100L);
        tenant1PublisherConfig.setNumberOfMessagesToSend(sendCount);

        AndesJMSPublisherClientConfiguration tenant2PublisherConfig = new AndesJMSPublisherClientConfiguration("tenant2user1!testtenant2.com", "tenant2user1", "127.0.0.1", 5672, ExchangeType.TOPIC, "testtenant2.com/commontopic");
        tenant2PublisherConfig.setPrintsPerMessageCount(100L);
        tenant2PublisherConfig.setNumberOfMessagesToSend(sendCount);


        AndesClient adminConsumerClient = new AndesClient(adminConsumerConfig);

        AndesClient tenant1ConsumerClient = new AndesClient(tenant1ConsumerConfig);

        AndesClient tenant2ConsumerClient = new AndesClient(tenant2ConsumerConfig);

        AndesClient adminPublisherClient = new AndesClient(adminPublisherConfig);

        AndesClient tenant1PublisherClient = new AndesClient(tenant1PublisherConfig);

        AndesClient tenant2PublisherClient = new AndesClient(tenant2PublisherConfig);

        adminConsumerClient.startClient();
        tenant1ConsumerClient.startClient();
        tenant2ConsumerClient.startClient();

        adminPublisherClient.startClient();
        tenant1PublisherClient.startClient();
        tenant2PublisherClient.startClient();

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(adminConsumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(tenant1ConsumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(tenant2ConsumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(adminPublisherClient.getSentMessageCount(), sendCount, "Sending failed for admin.");
        Assert.assertEquals(tenant1PublisherClient.getSentMessageCount(), sendCount, "Sending failed for tenant 1.");
        Assert.assertEquals(tenant2PublisherClient.getSentMessageCount(), sendCount, "Sending failed for tenant 2.");

        Assert.assertEquals(adminConsumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed for admin.");
        Assert.assertEquals(tenant1ConsumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed for tenant 1.");
        Assert.assertEquals(tenant2ConsumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed for tenant 2.");



//        int sendMessageCount = 100;
//        int runTime = 20;
//        int expectedMessageCount = 100;
//
//        // Start receiving clients (tenant1, tenant2 and admin)
//        AndesClientTemp tenant1ReceivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:commontopic",
//                "100", "false", Integer.toString(runTime), Integer.toString(expectedMessageCount),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedMessageCount, "",
//                "tenant1user1!testtenant1.com", "tenant1user1");
//        tenant1ReceivingClient.startWorking();
//
//        AndesClientTemp tenant2ReceivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:commontopic",
//                "100", "false", Integer.toString(runTime), Integer.toString(expectedMessageCount),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedMessageCount, "",
//                "tenant2user1!testtenant2.com", "tenant2user1");
//        tenant2ReceivingClient.startWorking();
//
//        AndesClientTemp adminReceivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:commontopic",
//                "100", "false", Integer.toString(runTime), Integer.toString(expectedMessageCount),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedMessageCount, "");
//        adminReceivingClient.startWorking();
//
//        // Start sending clients (tenant1, tenant2 and admin)
//        AndesClientTemp tenant1SendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:commontopic",
//                "100", "false", Integer.toString(runTime), Integer.toString(sendMessageCount), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendMessageCount, "",
//                "tenant1user1!testtenant1.com", "tenant1user1");
//
//        AndesClientTemp tenant2SendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:commontopic",
//                "100", "false", Integer.toString(runTime), Integer.toString(sendMessageCount), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendMessageCount, "",
//                "tenant2user1!testtenant2.com", "tenant2user1");
//
//        AndesClientTemp adminSendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:commontopic",
//                "100", "false", Integer.toString(runTime), Integer.toString(sendMessageCount), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendMessageCount, "");
//
//        tenant1SendingClient.startWorking();
//        tenant2SendingClient.startWorking();
//        adminSendingClient.startWorking();
//
//        boolean tenet1ReceiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(tenant1ReceivingClient,
//                expectedMessageCount, runTime);
//        boolean tenet2ReceiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(tenant2ReceivingClient,
//                expectedMessageCount, runTime);
//        boolean adminReceiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(adminReceivingClient,
//                expectedMessageCount, runTime);
//
//        boolean tenant1SendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(tenant1SendingClient, sendMessageCount);
//        boolean tenant2SendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(tenant2SendingClient, sendMessageCount);
//        boolean adminSendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(adminSendingClient, sendMessageCount);
//
//        Assert.assertTrue(tenant1SendSuccess, "Sending failed for tenant 1.");
//        Assert.assertTrue(tenant2SendSuccess, "Sending failed for tenant 2.");
//        Assert.assertTrue(adminSendSuccess, "Sending failed for admin.");
//        Assert.assertTrue(tenet1ReceiveSuccess, "Message receiving failed for tenant 1.");
//        Assert.assertTrue(tenet2ReceiveSuccess, "Message receiving failed for tenant 2.");
//        Assert.assertTrue(adminReceiveSuccess, "Message receiving failed for admin.");
    }
}
