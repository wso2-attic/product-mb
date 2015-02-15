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
 * Test topic subscriptions with Topic and Children(#) and Immediate Children(*).
 */
public class HierarchicalTopicsTestCase extends MBIntegrationBaseTest {
    private static final long EXPECTED_COUNT = 1000L;
    private static final long SEND_COUNT = 1000L;


//    static Integer sendCount = 1000;
//    static Integer runTime = 20;
//    static Integer expectedCount = 1000;


    @BeforeClass
    public void prepare() {
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * Testing on subscribers without wildcard characters
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void performHierarchicalTopicsTopicOnlyTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

        /**
         * topic only option. Here we subscribe to games.cricket and verify that only messages
         * specifically published to games.cricket is received
         */
        //we should not get any message here
        AndesClient receivingClient1 = getConsumerClientForTopic("games.cricket");
        receivingClient1.startClient();

        AndesClient sendingClient1 = getPublishingClientForTopic("games");
        sendingClient1.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient1, AndesClientConstants.DEFAULT_RUN_TIME);

        //now we send messages specific to games.cricket topic. We should receive messages here
        AndesClientUtils.sleepForInterval(1000);

        AndesClient receivingClient2 = getConsumerClientForTopic("games.cricket");
        receivingClient2.startClient();

        AndesClient sendingClient2 = getPublishingClientForTopic("games.cricket");
        sendingClient2.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(receivingClient2, AndesClientConstants.DEFAULT_RUN_TIME);

        // evaluating publishers
        Assert.assertEquals(sendingClient1.getSentMessageCount(), SEND_COUNT, "Publisher client1 failed to publish messages");
        Assert.assertEquals(sendingClient2.getSentMessageCount(), SEND_COUNT, "Publisher client2 failed to publish messages");

        // evaluating consumers
        Assert.assertNotEquals(receivingClient1.getReceivedMessageCount(), EXPECTED_COUNT, "Messages received when subscriber should not receive messages.");
        Assert.assertEquals(receivingClient1.getReceivedMessageCount(), 0, "Messages received when subscriber should not receive messages.");
        Assert.assertEquals(receivingClient2.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive messages for games.cricket.");
    }


    /**
     * immediate children option. Here you subscribe to the first level of sub-topics but not to the topic itself.
     * 1. subscribe to games.* and publish to games. Should receive no message
     * 2. subscribe to games.* and publish to games.football. Messages should be received
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void performHierarchicalTopicsImmediateChildrenTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

        //we should not get any message here
        AndesClient consumerClient3 = getConsumerClientForTopic("games.*");
        consumerClient3.startClient();

        AndesClient publisherClient3 = getPublishingClientForTopic("games");
        publisherClient3.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient3, AndesClientConstants.DEFAULT_RUN_TIME);

        //now we send messages child to games.football. We should receive messages here
        AndesClientUtils.sleepForInterval(1000);

        AndesClient consumerClient4 = getConsumerClientForTopic("games.*");
        consumerClient4.startClient();

        AndesClient publisherClient4 = getPublishingClientForTopic("games.football");
        publisherClient4.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient4, AndesClientConstants.DEFAULT_RUN_TIME);

        //now we send messages to a child that is not immediate. We should not receive messages
        AndesClientUtils.sleepForInterval(1000);

        AndesClient consumerClient5 = getConsumerClientForTopic("games.*");
        consumerClient5.startClient();

        AndesClient publisherClient5 = getPublishingClientForTopic("games.cricket.sl");
        publisherClient5.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient4, AndesClientConstants.DEFAULT_RUN_TIME);


        // evaluating publishers
        Assert.assertEquals(publisherClient3.getSentMessageCount(), SEND_COUNT, "Publisher publisherClient3 failed to publish messages");
        Assert.assertEquals(publisherClient4.getSentMessageCount(), SEND_COUNT, "Publisher publisherClient4 failed to publish messages");
        Assert.assertEquals(publisherClient5.getSentMessageCount(), SEND_COUNT, "Publisher publisherClient5 failed to publish messages");

        // evaluating consumers
        Assert.assertNotEquals(consumerClient3.getReceivedMessageCount(), EXPECTED_COUNT, "Messages received when subscriber consumerClient3 should not receive messages.");
        Assert.assertEquals(consumerClient3.getReceivedMessageCount(), 0, "Messages received when subscriber consumerClient3 should not receive messages.");

        Assert.assertEquals(consumerClient4.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive messages for consumerClient4.");

        Assert.assertNotEquals(consumerClient5.getReceivedMessageCount(), EXPECTED_COUNT, "Messages received when subscriber consumerClient5 should not receive messages.");
        Assert.assertEquals(consumerClient5.getReceivedMessageCount(), 0, "Messages received when subscriber consumerClient5 should not receive messages.");


    }

    /**
     * topic and children option. Here messages published to topic itself and any level
     * in the hierarchy should be received
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void performHierarchicalTopicsChildrenTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

        //we should  get any message here
        AndesClient consumerClient6 = getConsumerClientForTopic("games.#");
        consumerClient6.startClient();

        AndesClient publisherClient6 = getPublishingClientForTopic("games");
        publisherClient6.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient6, AndesClientConstants.DEFAULT_RUN_TIME);

        //now we send messages to level 2 child. We should receive messages here
        AndesClientUtils.sleepForInterval(1000);

        AndesClient consumerClient7 = getConsumerClientForTopic("games.#");
        consumerClient7.startClient();

        AndesClient publisherClient7 = getPublishingClientForTopic("games.football.sl");
        publisherClient7.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient7, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient6.getSentMessageCount(), SEND_COUNT, "Publisher publisherClient6 failed to publish messages");
        Assert.assertEquals(publisherClient7.getSentMessageCount(), SEND_COUNT, "Publisher publisherClient7 failed to publish messages");

        Assert.assertEquals(consumerClient6.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive messages for consumerClient6.");
        Assert.assertEquals(consumerClient7.getReceivedMessageCount(), EXPECTED_COUNT, "Did not receive messages for consumerClient7.");
    }

    private AndesClient getConsumerClientForTopic(String topicName)
            throws AndesClientException, JMSException, NamingException {
        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, topicName);
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(100L);

        return new AndesClient(consumerConfig);

//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:" + topicName,
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//        return receivingClient;
    }

    private AndesClient getPublishingClientForTopic(String topicName)
            throws AndesClientException, JMSException, NamingException {
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, topicName);
        publisherConfig.setPrintsPerMessageCount(100L);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        return new AndesClient(publisherConfig);

//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:" + topicName, "100", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=1000", "");
//        return sendingClient;
    }
}
