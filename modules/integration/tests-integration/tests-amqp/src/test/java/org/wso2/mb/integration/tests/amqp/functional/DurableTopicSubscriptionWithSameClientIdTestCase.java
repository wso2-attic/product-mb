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

package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.engine.frameworkutils.FrameworkPathUtil;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.File;
import java.io.IOException;


/**
 * This class holds test case to verify if shared durable topic subscriptions.
 * Shared durable topic subscriptions has enabled in broker.xml and tested in following test class.
 */
public class DurableTopicSubscriptionWithSameClientIdTestCase extends MBIntegrationBaseTest {

    /**
     * Expected amount set to more than what is received as the amount of messages received by the subscribers are unknown but the total should be the same amount as sent
     */
    private static final long EXPECTED_COUNT = 500L;
    private static final long SEND_COUNT = 12L;

    /**
     * Prepare environment for durable topic subscription with same client Id tests
     *
     * @throws Exception
     */
    @BeforeClass
    public void prepare() throws Exception {

        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);

        super.serverManager = new ServerConfigurationManager(automationContext);

        // Replace the broker.xml with the allowSharedTopicSubscriptions configuration enabled under amqp
        // and restarts the server.
        super.serverManager.applyConfiguration(new File(FrameworkPathUtil.getSystemResourceLocation() + File.separator +
                                                        "artifacts" + File.separator + "mb" + File.separator + "config" + File.separator +
                                                        "allowSharedTopicSubscriptionsConfig" + File.separator + "broker.xml"),
                                               new File(ServerConfigurationManager.getCarbonHome() +
                                                        File.separator + "repository" + File.separator + "conf" + File.separator + "broker.xml"),
                                               true, true);

    }


    /**
     * Start 3 durable subscribers. Start publisher which sends 12 messages.
     * Get the total count received by all durable subscribers and compare with sent message count of the publisher.
     */
    @Test(groups = {"wso2.mb", "durableTopic"})
    public void performDurableTopicWithSameClientIdTestCase()
            throws ClientConfigurationException, NamingException, JMSException, IOException,
                   CloneNotSupportedException {

        // Creating a JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "durableTopicSameClientID");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setDurable(true, "sameClientIDSub1");

        // Creating a JMS consumer client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "durableTopicSameClientID");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        // Creating clients
        AndesClient consumerClient1 = new AndesClient(consumerConfig, true);
        consumerClient1.startClient();

        AndesClient consumerClient2 = new AndesClient(consumerConfig, true);
        consumerClient2.startClient();

        AndesClient consumerClient3 = new AndesClient(consumerConfig, true);
        consumerClient3.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient3, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed.");
        long totalReceivingMessageCount = consumerClient1.getReceivedMessageCount() + consumerClient2.getReceivedMessageCount() + consumerClient3.getReceivedMessageCount();
        Assert.assertEquals(totalReceivingMessageCount, SEND_COUNT, "Message receive count not equal to sent message count.");

    }
}
