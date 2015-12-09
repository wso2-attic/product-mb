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
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.andes.stub.admin.types.Queue;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;
import org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.clients.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

/**
 * Class for testing the queue creation by a publisher when a publisher publishes to a non existing queue/topic.
 *
 * A new queue should be created and the messages should be stored when a queue sender publishes messages to a
 * non-existing queue. Messages should be discarded when a topic publisher sends messages to a non-existing topic.
 */
public class PublisherQueueCreationTestCase extends MBIntegrationBaseTest {

    /**
     * Initializing test case
     *
     * @throws XPathExpressionException
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws XPathExpressionException {
        super.init(TestUserMode.SUPER_TENANT_USER);
    }

    /**
     * 1. Publish 100 messages to a non-existing queue, "publisherQueue".
     * 2. Test the existence of the queue
     * 3. Check if messages are stored
     * 4. Connect a subscriber, the subscriber should receive messages
     *
     * @throws AndesClientConfigurationException
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     * @throws AndesClientException
     * @throws XPathExpressionException
     * @throws AutomationUtilException
     * @throws AndesAdminServiceBrokerManagerAdminException
     */
    @Test(groups = "wso2.mb", description = "Automatically create queue when publishing test case")
    public void publishToNonExistingQueueTestCase()
            throws AndesClientConfigurationException, JMSException, NamingException, IOException,
            AndesClientException, XPathExpressionException, AutomationUtilException,
            AndesAdminServiceBrokerManagerAdminException {

        long sendCount = 100L;
        long expectedCount = 100L;
        final String QUEUE_NAME = "publisherQueue";
        LoginLogoutClient loginLogoutClient = new LoginLogoutClient(super.automationContext);
        String sessionCookie = loginLogoutClient.login();
        AndesAdminClient admin = new AndesAdminClient(super.backendURL, sessionCookie);

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(), ExchangeType.QUEUE, QUEUE_NAME);
        consumerConfig.setMaximumMessagesToReceived(expectedCount);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);
        consumerConfig.setAsync(false);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(), ExchangeType.QUEUE, QUEUE_NAME);
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

        // Creating publisher and publishing messages
        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();
        AndesClientUtils.waitForMessages(publisherClient,AndesClientConstants.DEFAULT_RUN_TIME);

        // Asserting the existence of the queue
        Queue queue = admin.getQueueByName(QUEUE_NAME);
        Assert.assertNotNull(queue, "Queue is not created");
        Assert.assertEquals(queue.getMessageCount(), sendCount, "Messages are not stored in the queue");

        // Creating consumer and receiving messages
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Asserting the number of messages received
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }

    /**
     * 1. Publish 100 messages to a non-existing topic, "publisherTopic".
     * 2. Connect a subscriber, the subscriber should not receive any messages
     *
     * @throws AndesClientConfigurationException
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     * @throws AndesClientException
     * @throws XPathExpressionException
     * @throws AutomationUtilException
     * @throws AndesAdminServiceBrokerManagerAdminException
     */
    @Test(groups = "wso2.mb", description = "Do not create topic when publishing test case")
    public void publishToNonExistingTopicTestCase()
            throws AndesClientConfigurationException, JMSException, NamingException, IOException,
            AndesClientException, XPathExpressionException, AutomationUtilException,
            AndesAdminServiceBrokerManagerAdminException {

        long sendCount = 100L;
        long expectedCount = 0L;
        final String TOPIC_NAME = "publisherTopic";

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(), ExchangeType.TOPIC, TOPIC_NAME);
        consumerConfig.setMaximumMessagesToReceived(1);
        consumerConfig.setAsync(false);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(), ExchangeType.TOPIC, TOPIC_NAME);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);
        publisherConfig.setNumberOfMessagesToSend(sendCount);

        // Creating publisher and publishing messages
        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();
        AndesClientUtils.waitForMessages(publisherClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Creating consumer and receiving messages
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount, "Message receiving failed.");
    }
}
