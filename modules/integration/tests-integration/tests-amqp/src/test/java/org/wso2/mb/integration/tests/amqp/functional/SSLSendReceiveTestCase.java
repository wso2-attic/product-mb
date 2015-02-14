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
import java.io.File;
import java.io.IOException;


/**
 * send messages using SSL and receive messages using SSL
 */
public class SSLSendReceiveTestCase extends MBIntegrationBaseTest {

    private static final long EXPECTED_COUNT = 100L;
    private static final long SEND_COUNT = 100L;

    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue", "security"})
    public void performSingleQueueSendReceiveTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {
//        Integer sendCount = 100;
//        Integer runTime = 20;
//        Integer expectedCount = 100;
        String keyStorePath = System.getProperty("carbon.home") + File.separator + "repository" + File.separator
                + "resources" + File.separator + "security" + File.separator + "wso2carbon.jks";
        String trustStorePath = System.getProperty("carbon.home") + File.separator + "repository" + File.separator
                + "resources" + File.separator + "security" + File.separator + "client-truststore.jks";
        String keyStorePassword = "wso2carbon";
        String trustStorePassword = "wso2carbon";
        String sslConnectionURL = "amqp://admin:admin@carbon/carbon?brokerlist='tcp://localhost:8672?ssl='true'" +
                "&ssl_cert_alias='RootCA'&trust_store='" + trustStorePath + "'&trust_store_password='" +
                trustStorePassword
                + "'&key_store='" + keyStorePath + "'&key_store_password='" + keyStorePassword + "''";


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(sslConnectionURL, ExchangeType.QUEUE, "SSLSingleQueue");
        // Use a listener
        consumerConfig.setAsync(true);
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(sslConnectionURL, ExchangeType.QUEUE, "SSLSingleQueue");
        publisherConfig.setPrintsPerMessageCount(10L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);



        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();


        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receive error from consumerClient");


//
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:8672", "queue:SSLSingleQueue",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, sslConnectionURL);
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:SSLSingleQueue", "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, sslConnectionURL);
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        assertTrue(sendSuccess, "Message sending failed.");
//        assertTrue(receiveSuccess, "Message receiving failed.");
    }
}
