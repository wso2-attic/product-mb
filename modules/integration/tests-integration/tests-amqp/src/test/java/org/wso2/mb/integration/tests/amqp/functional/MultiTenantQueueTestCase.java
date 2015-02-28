/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;


/**
 * Testing for multi tenant - Queue specific test case
 * <p/>
 * Test case 1
 * 1. Start a client in a tenant(Normal tenant) which listens to a queue
 * 2. Send 200 messages to the queue
 * 3. Client should receive all 200 messages
 * <p/>
 * Test case 2
 * 1. Start 2 receiving clients from different tenant for the same queue
 * 2. Start 2 sending clients from different tenant for the same queue
 * 3. Receiving clients should receive the message from their tenant only
 */
public class MultiTenantQueueTestCase extends MBIntegrationBaseTest {

    /**
     * Initializes the test case.
     *
     * @throws XPathExpressionException
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws XPathExpressionException {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * Test case 1
     * 1. Start a client in a tenant(Normal tenant) which listens to a queue
     * 2. Send 200 messages to the queue
     * 3. Client should receive all 200 messages
     *
     * @throws JMSException
     * @throws IOException
     * @throws NamingException
     * @throws AndesClientConfigurationException
     */
    @Test(groups = "wso2.mb", description = "Single Tenant Test")
    public void performSingleTenantMultipleUserQueueTestCase()
            throws JMSException, IOException, NamingException, AndesClientConfigurationException {
        int sendMessageCount = 200;
        int expectedMessageCount = 200;

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration adminConsumerConfig = new AndesJMSConsumerClientConfiguration("admin!topictenant1.com", "admin", "127.0.0.1", 5672, ExchangeType.QUEUE, "topictenant1.com/tenantQueue");
        adminConsumerConfig.setMaximumMessagesToReceived(expectedMessageCount);
        adminConsumerConfig.setPrintsPerMessageCount(expectedMessageCount / 10L);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration tenantPublisherConfig = new AndesJMSPublisherClientConfiguration("topictenantuser1!topictenant1.com", "topictenantuser1", "127.0.0.1", 5672, ExchangeType.QUEUE, "topictenant1.com/tenantQueue");
        tenantPublisherConfig.setNumberOfMessagesToSend(sendMessageCount);
        tenantPublisherConfig.setPrintsPerMessageCount(sendMessageCount / 10L);

        // Creating clients
        AndesClient adminConsumerClient = new AndesClient(adminConsumerConfig, true);
        adminConsumerClient.startClient();

        AndesClient tenantPublisherClient = new AndesClient(tenantPublisherConfig, true);
        tenantPublisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(adminConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(tenantPublisherClient.getSentMessageCount(), sendMessageCount, "Sending failed for tenant 1 user 1.");
        Assert.assertEquals(adminConsumerClient.getReceivedMessageCount(), expectedMessageCount, "Message receiving failed for admin of tenant 1.");
    }

    /**
     * Test case 2
     * 1. Start 2 receiving clients from different tenant for the same queue
     * 2. Start 2 sending clients from different tenant for the same queue
     * 3. Receiving clients should receive the message from their tenant only
     *
     * @throws AndesClientConfigurationException
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     */
    @Test(groups = "wso2.mb", description = "Multiple Tenant Single Users Test")
    public void performMultipleTenantQueueTestCase()
            throws AndesClientConfigurationException, JMSException, NamingException, IOException {

        int sendMessageCount1 = 80;
        int sendMessageCount2 = 120;
        int expectedMessageCount = 200;

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration tenant1ConsumerConfig = new AndesJMSConsumerClientConfiguration("topictenantuser1!topictenant1.com", "topictenantuser1", "127.0.0.1", 5672, ExchangeType.QUEUE, "topictenant1.com/multitenantQueue");
        tenant1ConsumerConfig.setMaximumMessagesToReceived(expectedMessageCount);
        tenant1ConsumerConfig.setPrintsPerMessageCount(expectedMessageCount / 10L);

        AndesJMSConsumerClientConfiguration tenant2ConsumerConfig = new AndesJMSConsumerClientConfiguration("topictenantuser1!topictenant2.com", "topictenantuser1", "127.0.0.1", 5672, ExchangeType.QUEUE, "topictenant2.com/multitenantQueue");
        tenant2ConsumerConfig.setMaximumMessagesToReceived(expectedMessageCount);
        tenant2ConsumerConfig.setPrintsPerMessageCount(expectedMessageCount / 10L);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration tenant1PublisherConfig = new AndesJMSPublisherClientConfiguration("topictenantuser1!topictenant1.com", "topictenantuser1", "127.0.0.1", 5672, ExchangeType.QUEUE, "topictenant1.com/multitenantQueue");
        tenant1PublisherConfig.setNumberOfMessagesToSend(sendMessageCount1);
        tenant1PublisherConfig.setPrintsPerMessageCount(sendMessageCount1 / 10L);

        AndesJMSPublisherClientConfiguration tenant2PublisherConfig = new AndesJMSPublisherClientConfiguration("topictenantuser1!topictenant2.com", "topictenantuser1", "127.0.0.1", 5672, ExchangeType.QUEUE, "topictenant2.com/multitenantQueue");
        tenant2PublisherConfig.setNumberOfMessagesToSend(sendMessageCount2);
        tenant2PublisherConfig.setPrintsPerMessageCount(sendMessageCount2 / 10L);

        // Creating clients
        AndesClient tenant1ConsumerClient = new AndesClient(tenant1ConsumerConfig, true);
        tenant1ConsumerClient.startClient();

        AndesClient tenant2ConsumerClient = new AndesClient(tenant2ConsumerConfig, true);
        tenant2ConsumerClient.startClient();

        AndesClient tenant1PublisherClient = new AndesClient(tenant1PublisherConfig, true);
        tenant1PublisherClient.startClient();

        AndesClient tenant2PublisherClient = new AndesClient(tenant2PublisherConfig, true);
        tenant2PublisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(tenant1ConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(tenant2ConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(tenant1PublisherClient.getSentMessageCount(), sendMessageCount1, "Sending failed for tenant 1.");
        Assert.assertEquals(tenant2PublisherClient.getSentMessageCount(), sendMessageCount2, "Sending failed for tenant 2.");
        Assert.assertEquals(tenant1ConsumerClient.getReceivedMessageCount(), sendMessageCount1, "Tenant 1 client received incorrect number of message count.");
        Assert.assertEquals(tenant2ConsumerClient.getReceivedMessageCount(), sendMessageCount2, "Tenant 2 client received incorrect number of message count.");
    }
}
