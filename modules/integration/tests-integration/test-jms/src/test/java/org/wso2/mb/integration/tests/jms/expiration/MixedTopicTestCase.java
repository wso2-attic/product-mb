/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.mb.integration.tests.jms.expiration;

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
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;
import org.wso2.mb.integration.tests.JMSTestConstants;

import javax.jms.JMSException;
import javax.naming.NamingException;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

/**
 * This class includes unit tests to verify that messages with JMS expiration are properly removed when delivering to
 * non-durable topics.
 */
public class MixedTopicTestCase extends MBIntegrationBaseTest {

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * 1. Subscribe to a topic.
     * 2. Send messages with and without expiry as configured
     * 3. Verify that only messages without expiry have been received and that both types of messages have been sent.
     */
    @Test(groups = "wso2.mb", description = "Single topic send-receive test case with jms expiration")
    public void performSingleTopicSendReceiveTestCase()
            throws AndesClientException, NamingException, JMSException, IOException {

        long expectedMessageCountFromSubscriberSession = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT *
                (JMSTestConstants.SEND_MESSAGE_PERCENTAGE_WITHOUT_EXPIRY / 100);

        long messageCountWithExpiration = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants
                .SEND_MESSAGE_PERCENTAGE_WITH_EXPIRY / 100);

        long messageCountWithoutExpiration = JMSTestConstants.DEFAULT_TOTAL_SEND_MESSAGE_COUNT * (JMSTestConstants
                .SEND_MESSAGE_PERCENTAGE_WITHOUT_EXPIRY / 100);


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "jmsSingleTopic");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(expectedMessageCountFromSubscriberSession);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(expectedMessageCountFromSubscriberSession/10L);

        AndesJMSPublisherClientConfiguration publisherConfigWithoutExpiration = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "jmsSingleTopic");
        publisherConfigWithoutExpiration.setPrintsPerMessageCount(messageCountWithoutExpiration / 10L);
        publisherConfigWithoutExpiration.setNumberOfMessagesToSend(messageCountWithoutExpiration);

        AndesJMSPublisherClientConfiguration publisherConfigWithExpiration = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "jmsSingleTopic");
        publisherConfigWithExpiration.setPrintsPerMessageCount(messageCountWithExpiration / 10L);
        publisherConfigWithExpiration.setNumberOfMessagesToSend(messageCountWithExpiration);
        publisherConfigWithExpiration.setJMSMessageExpiryTime(100L);


        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClientWithoutExpiration = new AndesClient(publisherConfigWithoutExpiration);
        publisherClientWithoutExpiration.startClient();

        AndesClient publisherClientWithExpiration = new AndesClient(publisherConfigWithExpiration);
        publisherClientWithExpiration.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClientWithExpiration.getSentMessageCount(), messageCountWithoutExpiration, "Message send failed");
        Assert.assertEquals(publisherClientWithExpiration.getSentMessageCount(), messageCountWithExpiration, "Message send failed");

        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedMessageCountFromSubscriberSession, "Message receiving failed.");





//        AndesClient receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:singleTopic",
//                "100", "false", JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(),
//                expectedMessageCountFromSubscriberSession.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0," +
//                "stopAfter=" + expectedMessageCountFromSubscriberSession, "");
//
//        receivingClient.startWorking();
//
//        AndesClient sendingClient1 = new AndesClient("send", "127.0.0.1:5672", "topic:singleTopic", "100", "false",
//                JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(),
//                messageCountWithoutExpiration.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + messageCountWithoutExpiration, "");
//
//        sendingClient1.startWorking();
//
//        AndesClient sendingClient2 = new AndesClientTemp("send", "127.0.0.1:5672", "topic:singleTopic", "100", "false",
//                JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(), messageCountWithExpiration.toString()
//                , "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + messageCountWithExpiration + "," +
//                        "jmsExpiration=" + JMSTestConstants.SAMPLE_JMS_EXPIRATION, "");
//
//        sendingClient2.startWorking();
//
//        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient,
//                expectedMessageCountFromSubscriberSession, JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS);
//
//        boolean sendSuccess1 = AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, messageCountWithoutExpiration);
//
//        boolean sendSuccess2 = AndesClientUtils.getIfPublisherIsSuccess(sendingClient2, messageCountWithExpiration);
//
//        assertEquals((receiveSuccess && sendSuccess1 && sendSuccess2), true);
    }

}
