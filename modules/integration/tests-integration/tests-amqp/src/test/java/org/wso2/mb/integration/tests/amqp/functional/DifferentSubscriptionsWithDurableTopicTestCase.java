package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

/**
 * This test class with perform test case of having different types of subscriptions together with
 * durable topic subscription.
 */
public class DifferentSubscriptionsWithDurableTopicTestCase extends MBIntegrationBaseTest {
    /**
     * Sent message count.
     */
    private static final long SEND_COUNT = 1000L;

    /**
     * Expected message count.
     */
    private static final long EXPECTED_COUNT = SEND_COUNT;

    /**
     * Topic name to publish and receive.
     */
    private static final String TOPIC_NAME = "a.b.c";

    /**
     * Hierarchical topic name to publish and receive.
     */
    private static final String HIERARCHICAL_TOPIC = "a.b.*";

    /**
     * Initializing test case
     */
    @BeforeClass
    public void prepare() throws XPathExpressionException {
        init(TestUserMode.SUPER_TENANT_ADMIN);
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * Create different type of subscribers/consumers and publish {@link #SEND_COUNT} messages to
     * {@link #TOPIC_NAME}. All topic subscriptions will received message {@link #EXPECTED_COUNT}
     * messages. Queue subscription should not received any messages.
     *
     * @throws AndesClientConfigurationException
     * @throws CloneNotSupportedException
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = {"wso2.mb", "durableTopic"})
    public void performDifferentTopicSubscriptionsWithDurableTopicTest()
            throws AndesClientConfigurationException, CloneNotSupportedException, JMSException,
                   NamingException,
                   IOException, AndesClientException {

        // Creating a consumer client configurations
        AndesJMSConsumerClientConfiguration durableTopicConsumerConfig1 = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, TOPIC_NAME);
        durableTopicConsumerConfig1.setMaximumMessagesToReceived(EXPECTED_COUNT);
        durableTopicConsumerConfig1.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);
        durableTopicConsumerConfig1.setDurable(true, "diffSub1"); // durable topic

        AndesJMSConsumerClientConfiguration durableTopicConsumerConfig2 = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, TOPIC_NAME);
        durableTopicConsumerConfig2.setMaximumMessagesToReceived(EXPECTED_COUNT);
        durableTopicConsumerConfig2.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);
        durableTopicConsumerConfig2.setDurable(true, "diffSub2"); // durable topic

        AndesJMSConsumerClientConfiguration normalTopicConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, TOPIC_NAME);
        normalTopicConsumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        normalTopicConsumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);

        AndesJMSConsumerClientConfiguration normalHierarchicalTopicConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, HIERARCHICAL_TOPIC);
        normalHierarchicalTopicConsumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        normalHierarchicalTopicConsumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);

        AndesJMSConsumerClientConfiguration durableHierarchicalTopicConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, HIERARCHICAL_TOPIC);
        durableHierarchicalTopicConsumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        durableHierarchicalTopicConsumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT / 10L);
        durableHierarchicalTopicConsumerConfig.setDurable(true, "diffSub3"); // durable topic

        AndesJMSConsumerClientConfiguration queueConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, TOPIC_NAME); // queue consumer
        queueConsumerConfig.setMaximumMessagesToReceived(10L);  // To wait if any message does received

        // Creating a publisher client configurations
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, TOPIC_NAME);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setPrintsPerMessageCount(SEND_COUNT / 10L);

        // Creating clients
        AndesClient durableTopicConsumerClient1 = new AndesClient(durableTopicConsumerConfig1, true);
        durableTopicConsumerClient1.startClient();

        AndesClient durableTopicConsumerClient2 = new AndesClient(durableTopicConsumerConfig2, true);
        durableTopicConsumerClient2.startClient();

        AndesClient normalTopicConsumerClient = new AndesClient(normalTopicConsumerConfig, true);
        normalTopicConsumerClient.startClient();

        AndesClient normalHierarchicalTopicConsumerClient = new AndesClient(normalHierarchicalTopicConsumerConfig, true);
        normalHierarchicalTopicConsumerClient.startClient();

        AndesClient durableHierarchicalTopicConsumerClient = new AndesClient(durableHierarchicalTopicConsumerConfig, true);
        durableHierarchicalTopicConsumerClient.startClient();

        AndesClient queueConsumerClient = new AndesClient(queueConsumerConfig, true);
        queueConsumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.sleepForInterval(4000L);

        // Evaluation
        AndesClientUtils.waitForMessagesAndShutdown(durableTopicConsumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(durableTopicConsumerClient1.getReceivedMessageCount(), EXPECTED_COUNT, "Message receive error from durable subscriber 1");

        AndesClientUtils.waitForMessagesAndShutdown(durableTopicConsumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(durableTopicConsumerClient2.getReceivedMessageCount(), EXPECTED_COUNT, "Message receive error from durable subscriber 2");

        AndesClientUtils.waitForMessagesAndShutdown(normalTopicConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(normalTopicConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receive error from normal topic subscriber");

        AndesClientUtils.waitForMessagesAndShutdown(normalHierarchicalTopicConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(normalHierarchicalTopicConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT,
                            "Message receive error from normal hierarchical topic subscriber");

        AndesClientUtils.waitForMessagesAndShutdown(durableHierarchicalTopicConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(durableHierarchicalTopicConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT,
                            "Message receive error from durable hierarchical topic subscriber");

        AndesClientUtils.waitForMessagesAndShutdown(queueConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(queueConsumerClient.getReceivedMessageCount(), 0L,
                            "Message received from queue subscriber. This should not happen");

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT,
                            "Message send error");
    }
}

