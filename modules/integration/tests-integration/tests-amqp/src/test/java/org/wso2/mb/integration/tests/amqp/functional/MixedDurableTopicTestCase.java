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
 * Unit tests to ensure jms expiration works as expected with durable topics.
 */
public class MixedDurableTopicTestCase extends MBIntegrationBaseTest {

    private static final long SEND_COUNT = 1000L;
    private static final long SEND_COUNT_WITHOUT_EXPIRATION = 600L;
    private static final long SEND_COUNT_WITH_EXPIRATION = SEND_COUNT - SEND_COUNT_WITHOUT_EXPIRATION;
    private static final long EXPECTED_COUNT_BY_ONE_SUBSCRIBER = SEND_COUNT_WITHOUT_EXPIRATION / 2L;
    private static final long EXPIRATION_TIME = 5L;

    @BeforeClass
    public void prepare() {
        // Any initialisations / configurations should be made at this point.
        AndesClientUtils.sleepForInterval(15000);
    }


    /**
     * 1. Start durable topic subscriber
     * 2. Send 1000 messages with 400 messages having expiration
     * 3. Stop subscriber after receiving 300 messages
     * 4. Start subscriber
     * 5. Verify that the subscriber has received remaining 300 messages.
     * 6. Pass test case if and only if 600 messages in total have been received.
     */
    @Test(groups = "wso2.mb", description = "Single durable topic send-receive test case with jms expiration")
    public void performExpiryDurableTopicTestCase()
            throws AndesClientException, NamingException, JMSException, IOException,
                   CloneNotSupportedException {


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "durableTopicWithExpiration");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT_BY_ONE_SUBSCRIBER);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT_BY_ONE_SUBSCRIBER / 10L);
        consumerConfig.setDurable(true, "expirationSub");

        AndesJMSPublisherClientConfiguration publisherConfigWithoutExpiration = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "durableTopicWithExpiration");
        publisherConfigWithoutExpiration.setNumberOfMessagesToSend(SEND_COUNT_WITHOUT_EXPIRATION);
        publisherConfigWithoutExpiration.setPrintsPerMessageCount(SEND_COUNT_WITHOUT_EXPIRATION / 10L);

        AndesJMSPublisherClientConfiguration publisherConfigWithExpiration = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "durableTopicWithExpiration");
        publisherConfigWithExpiration.setNumberOfMessagesToSend(SEND_COUNT_WITH_EXPIRATION);
        publisherConfigWithExpiration.setPrintsPerMessageCount(SEND_COUNT_WITH_EXPIRATION / 10L);
        publisherConfigWithExpiration.setJMSMessageExpiryTime(EXPIRATION_TIME);


        AndesClient initialConsumerClient = new AndesClient(consumerConfig);
        initialConsumerClient.startClient();

        AndesClient publisherClientWithoutExpiration = new AndesClient(publisherConfigWithoutExpiration);
        publisherClientWithoutExpiration.startClient();

        AndesClient publisherClientWithExpiration = new AndesClient(publisherConfigWithExpiration);
        publisherClientWithExpiration.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(initialConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        AndesJMSConsumerClientConfiguration secondaryConsumerConfig = consumerConfig.clone();
        secondaryConsumerConfig.setUnSubscribeAfterEachMessageCount(EXPECTED_COUNT_BY_ONE_SUBSCRIBER);
        secondaryConsumerConfig.setMaximumMessagesToReceived(Long.MAX_VALUE);
        AndesClient secondaryConsumerClient = new AndesClient(secondaryConsumerConfig);
        secondaryConsumerClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(secondaryConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        AndesClient tertiaryConsumerClient = new AndesClient(consumerConfig);
        tertiaryConsumerClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(tertiaryConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClientWithoutExpiration.getSentMessageCount(), SEND_COUNT_WITHOUT_EXPIRATION, "Message send failed for publisherClientWithoutExpiration");
        Assert.assertEquals(publisherClientWithExpiration.getSentMessageCount(), SEND_COUNT_WITH_EXPIRATION, "Message send failed for publisherClientWithExpiration");

        Assert.assertEquals(initialConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT_BY_ONE_SUBSCRIBER, "Message receiving failed for initialConsumerClient");
        Assert.assertEquals(secondaryConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT_BY_ONE_SUBSCRIBER, "Message receiving failed for secondaryConsumerClient");
        Assert.assertEquals(tertiaryConsumerClient.getReceivedMessageCount(), 0L, "Message receiving failed for tertiaryConsumerClient");


//        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
//                "100", "false", JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(), expectedMessageCountFromOneSubscriberSession.toString(),
//                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0,stopAfter="+expectedMessageCountFromOneSubscriberSession.toString(), "");
//
//        receivingClient.startWorking();
//
//        AndesClient sendingClient1 = new AndesClient("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false"
//                , JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(), messageCountWithoutExpiration.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter="+messageCountWithoutExpiration.toString(), "");
//
//        sendingClient1.startWorking();
//
//        AndesClient sendingClient2 = new AndesClient("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false",
//                JMSTestConstants.DEFAULT_SENDER_RUN_TIME_IN_SECONDS.toString(), messageCountWithExpiration.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter="+messageCountWithExpiration +",jmsExpiration="+ JMSTestConstants.SAMPLE_JMS_EXPIRATION, "");
//
//        sendingClient2.startWorking();
//
//        boolean receivingSuccess1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedMessageCountFromOneSubscriberSession , JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS);
//
//        boolean sendingSuccess1 = AndesClientUtils.getIfPublisherIsSuccess(sendingClient1, messageCountWithoutExpiration);
//        boolean sendingSuccess2 = AndesClientUtils.getIfPublisherIsSuccess(sendingClient2, messageCountWithExpiration);
//
//        //we just closed the subscription. Rest of messages should be delivered now.
//
//        AndesClientUtils.sleepForInterval(2000);
//
//        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
//                "100", "false", JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(), expectedMessageCountFromOneSubscriberSession.toString(),
//                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0,unsubscribeAfter="+expectedMessageCountFromOneSubscriberSession+",stopAfter="+expectedMessageCountFromOneSubscriberSession, "");
//        receivingClient2.startWorking();
//
//        boolean receivingSuccess2 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2, expectedMessageCountFromOneSubscriberSession , JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS);
//
//        //now we have unsubscribed the topic subscriber no more messages should be received
//        AndesClientUtils.sleepForInterval(2000);
//
//        AndesClient receivingClient3 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
//                "100", "false", JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS.toString(), expectedMessageCountFromOneSubscriberSession.toString(),
//                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0,unsubscribeAfter="+expectedMessageCountFromOneSubscriberSession+",stopAfter="+expectedMessageCountFromOneSubscriberSession, "");
//        receivingClient3.startWorking();
//
//        boolean receivingSuccess3 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3, 0 , JMSTestConstants.DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS);
//
//        Assert.assertEquals((sendingSuccess1 && sendingSuccess2 && receivingSuccess1 && receivingSuccess2 && !receivingSuccess3), true);
    }
}
