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
 * This class includes unit tests to verify that messages with JMS expiration are properly removed when delivering to
 * non-durable topics.
 */
public class MixedTopicTestCase extends MBIntegrationBaseTest {

    private static final long SEND_COUNT = 1000L;
    private static final long SEND_COUNT_WITHOUT_EXPIRATION = 600L;
    private static final long SEND_COUNT_WITH_EXPIRATION = SEND_COUNT - SEND_COUNT_WITHOUT_EXPIRATION;
    private static final long EXPECTED_COUNT = SEND_COUNT_WITHOUT_EXPIRATION;
    private static final long EXPIRATION_TIME = 5L;

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
    public void performSingleExpiryTopicSendReceiveTestCase()
            throws AndesClientException, NamingException, JMSException, IOException {


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "topicWithExpiry");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT/10L);

        AndesJMSPublisherClientConfiguration publisherConfigWithoutExpiration = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "topicWithExpiry");
        publisherConfigWithoutExpiration.setPrintsPerMessageCount(SEND_COUNT_WITHOUT_EXPIRATION / 10L);
        publisherConfigWithoutExpiration.setNumberOfMessagesToSend(SEND_COUNT_WITHOUT_EXPIRATION);

        AndesJMSPublisherClientConfiguration publisherConfigWithExpiration = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "topicWithExpiry");
        publisherConfigWithExpiration.setPrintsPerMessageCount(SEND_COUNT_WITH_EXPIRATION / 10L);
        publisherConfigWithExpiration.setNumberOfMessagesToSend(SEND_COUNT_WITH_EXPIRATION);
        publisherConfigWithExpiration.setJMSMessageExpiryTime(EXPIRATION_TIME);


        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClientWithoutExpiration = new AndesClient(publisherConfigWithoutExpiration);
        publisherClientWithoutExpiration.startClient();

        AndesClient publisherClientWithExpiration = new AndesClient(publisherConfigWithExpiration);
        publisherClientWithExpiration.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient,  AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClientWithoutExpiration.getSentMessageCount(), SEND_COUNT_WITHOUT_EXPIRATION, "Message send failed for publisherClientWithoutExpiration");
        Assert.assertEquals(publisherClientWithExpiration.getSentMessageCount(), SEND_COUNT_WITH_EXPIRATION, "Message send failed for publisherClientWithExpiration");

        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed for consumerClient");





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
