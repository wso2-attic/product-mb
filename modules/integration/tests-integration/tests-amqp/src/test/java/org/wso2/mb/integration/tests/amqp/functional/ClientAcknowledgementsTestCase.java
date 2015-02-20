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
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

/**
 * This class includes test cases to test client acknowledgements modes for queues
 */
public class ClientAcknowledgementsTestCase extends MBIntegrationBaseTest {

    /**
     * Amount of messages sent.
     */
    private static final long SEND_COUNT = 1000L;

    /**
     * Amount of messages expected.
     */
    private static final long EXPECTED_COUNT = SEND_COUNT;

    /**
     * Initializing test case
     *
     * @throws XPathExpressionException
     */
    @BeforeClass
    public void prepare() throws XPathExpressionException {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * In this test it will check functionality of client acknowledgement by acknowledging bunch by
     * bunch.
     * 1. Start queue receiver in client acknowledge mode.
     * 2. Publisher sends {@link #SEND_COUNT} messages.
     * 3. Consumer receives messages and only acknowledge after each 200 messages.
     * 4. Consumer should receive {@link #EXPECTED_COUNT} messages.
     */
    @Test(groups = {"wso2.mb", "queue"})
    public void performClientAcknowledgementsTestCase()
            throws AndesClientException, JMSException, NamingException, IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "clientAckTestQueue");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE); // using client acknowledgement
        consumerConfig.setAcknowledgeAfterEachMessageCount(200L); // acknowledge a message only after 200 messages are received
        consumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);

        // Creating a JMS publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "clientAckTestQueue");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

        AndesClient consumerClient1 = new AndesClient(consumerConfig);
        consumerClient1.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);

        AndesClient consumerClient2 = new AndesClient(consumerConfig);
        consumerClient2.startClient();

        AndesClientUtils.sleepForInterval(2000);

        long totalMessagesReceived = consumerClient1.getReceivedMessageCount() + consumerClient2.getReceivedMessageCount();

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Expected message count not received.");
        Assert.assertEquals(totalMessagesReceived, EXPECTED_COUNT, "Expected message count not received.");
    }
}
