/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.platform.tests.clustering.durable.topic;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.topic.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

/**
 * This class performs tests related to message delivery of durable topics
 */
public class DurableTopicMessageDeliveringTestCase extends MBPlatformBaseTest{


    private AutomationContext automationContext1;
    private TopicAdminClient topicAdminClient1;
    private static final long SEND_COUNT = 500L;
    private static final long EXPECTED_COUNT = SEND_COUNT;


    /**
     * Prepare environment for tests.
     *
     * @throws Exception
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);

        automationContext1 = getAutomationContextWithKey("mb002");
        topicAdminClient1 = new TopicAdminClient(automationContext1.getContextUrls().getBackEndUrl(),
                super.login(automationContext1), ConfigurationContextProvider.getInstance().getConfigurationContext());

    }

    /**
     * Subscribe to a durable topic and publish messages to that topic
     * @throws Exception
     */
    @Test(groups = {"wso2.mb", "durableTopic"})
    public void pubSubDurableTopicTestCase() throws Exception{

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                     Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                     ExchangeType.TOPIC, "durableTopicMessageDelivering");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);
        consumerConfig.setDurable(true, "durableTopicSub5");
        consumerConfig.setUnSubscribeAfterEachMessageCount(500L);


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(automationContext1.getInstance().getHosts().get("default"),
                                                                                                        Integer.parseInt(automationContext1.getInstance().getPorts().get("amqp")),
                                                                                                        ExchangeType.TOPIC, "durableTopicMessageDelivering");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed.");


//        Integer sendCount = 500;
//        Integer runTime = 20;
//        Integer expectedCount = 500;
//
//        String hostInfoReceiver = automationContext1.getInstance().getHosts().get("default") +
//                ":" +
//                automationContext1.getInstance().getPorts().get("amqp");
//
//        AndesClient receivingClient = new AndesClient("receive", hostInfoReceiver,
//                "topic:durableTopic1",
//                "100", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,durable=true,subscriptionID=durableTopicSub5," +
//                "unsubscribeAfter=500," +
//                "delayBetweenMsg=0," +
//                "stopAfter=" + expectedCount, "");
//        receivingClient.startWorking();
//
//        String hostInfoSender = automationContext1.getInstance().getHosts().get("default") +
//                ":" +
//                automationContext1.getInstance().getPorts().get("amqp");
//        AndesClient sendingClient = new AndesClient("send", hostInfoSender,
//                "topic:durableTopic1", "100",
//                "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receivingSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount,
//                runTime);
//
//        boolean sendingSuccess = AndesClientUtils.getIfPublisherIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(receivingSuccess, "Did not receive all the messages");
//        Assert.assertTrue(sendingSuccess, "Messaging sending failed");

    }

    /**
     * Cleanup after running tests.
     *
     * @throws Exception
     */
    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {
        topicAdminClient1.removeTopic("durableTopic1");

    }
}
