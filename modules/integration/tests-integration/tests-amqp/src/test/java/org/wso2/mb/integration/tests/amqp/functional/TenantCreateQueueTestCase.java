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
 * This tests if a tenant user can create queues, send and receive messages.
 */
public class TenantCreateQueueTestCase extends MBIntegrationBaseTest {

    private static final long EXPECTED_COUNT = 100L;
    private static final long SEND_COUNT = 100L;

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
    }

    @Test(groups = "wso2.mb", description = "Single queue send-receive test case")
    public void performSingleQueueSendReceiveTestCase() throws IOException,
                                                               AndesClientException, JMSException,
                                                               NamingException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration("tenant1user1!testtenant1.com", "tenant1user1", "127.0.0.1", 5672, ExchangeType.QUEUE, "testtenant1.com/www");
        // Use a listener
        consumerConfig.setAsync(true);
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration("tenant1user1!testtenant1.com", "tenant1user1", "127.0.0.1", 5672, ExchangeType.QUEUE, "testtenant1.com/www");
        publisherConfig.setPrintsPerMessageCount(10L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);



        AndesJMSConsumerClient consumerClient = new AndesJMSConsumerClient(consumerConfig);
        consumerClient.startClient();

        AndesJMSPublisherClient publisherClient = new AndesJMSPublisherClient(publisherConfig);
        publisherClient.startClient();




        AndesClientUtils.sleepForInterval(10000);

        AndesClientUtils.waitUntilAllMessageReceivedAndShutdownClients(consumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);


        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "TENANT 1 send failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "TENANT 1 receive failed");



//        int sendMessageCount = 100;
//        int runTime = 20;
//        int expectedMessageCount = 100;
//
//        // Start receiving clients (tenant1, tenant2 and admin)
//        AndesClientTemp tenant1ReceivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:testtenant1.com/www",
//                "100", "false", Integer.toString(runTime), Integer.toString(expectedMessageCount),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedMessageCount, "",
//                "tenant1user1!testtenant1.com", "tenant1user1");
//        tenant1ReceivingClient.startWorking();
//
//        // Start sending clients (tenant1, tenant2 and admin)
//        AndesClientTemp tenant1SendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:testtenant1.com/www",
//                "100", "false", Integer.toString(runTime), Integer.toString(sendMessageCount), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendMessageCount, "",
//                "tenant1user1!testtenant1.com", "tenant1user1");
//
//        tenant1SendingClient.startWorking();
//        AndesClientUtils.sleepForInterval(10000);
//
//        boolean tenet1ReceiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(tenant1ReceivingClient,
//                                                                                         expectedMessageCount, runTime);
//
//        boolean tenant1SendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(tenant1SendingClient, sendMessageCount);
//
//        assertTrue(tenant1SendSuccess, "TENANT 1 send failed");
//        assertTrue(tenet1ReceiveSuccess, "TENANT 1 receive failed");
    }
}
