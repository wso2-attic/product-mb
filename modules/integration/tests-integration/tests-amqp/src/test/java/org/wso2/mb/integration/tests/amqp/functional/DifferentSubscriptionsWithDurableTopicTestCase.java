package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;

/**
 * This test class with perform test case of having different types of subscriptions together with
 * durable topic subscription.
 */
public class DifferentSubscriptionsWithDurableTopicTestCase {
    private static final long SEND_COUNT = 1000L;
    private static final long EXPECTED_COUNT = 500L;

    private static final String TOPIC_NAME = "a.b.c";
    private static final String HIERARCHICAL_TOPIC = "a.b.*";

    @Test(groups = {"wso2.mb", "durableTopic"})
    public void performDifferentTopicSubscriptionsWithDurableTopicTest()
            throws AndesClientException, CloneNotSupportedException, JMSException, NamingException,
                   IOException {

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration durableTopicConsumerConfig1 = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, TOPIC_NAME);
        // Use a listener
        durableTopicConsumerConfig1.setAsync(true);
        // Amount of message to receive
        durableTopicConsumerConfig1.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        durableTopicConsumerConfig1.setPrintsPerMessageCount(EXPECTED_COUNT/10L);
        durableTopicConsumerConfig1.setDurable(true, "diffSub1");

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration durableTopicConsumerConfig2 = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, TOPIC_NAME);
        // Use a listener
        durableTopicConsumerConfig2.setAsync(true);
        // Amount of message to receive
        durableTopicConsumerConfig2.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        durableTopicConsumerConfig2.setPrintsPerMessageCount(EXPECTED_COUNT/10L);
        durableTopicConsumerConfig2.setDurable(true, "diffSub2");


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration normalTopicConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, TOPIC_NAME);
        // Use a listener
        normalTopicConsumerConfig.setAsync(true);
        // Amount of message to receive
        normalTopicConsumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        normalTopicConsumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT/10L);


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration normalHierarchicalTopicConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, HIERARCHICAL_TOPIC);
        // Use a listener
        normalHierarchicalTopicConsumerConfig.setAsync(true);
        // Amount of message to receive
        normalHierarchicalTopicConsumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        normalHierarchicalTopicConsumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT/10L);


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration durableHierarchicalTopicConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, HIERARCHICAL_TOPIC);
        // Use a listener
        durableHierarchicalTopicConsumerConfig.setAsync(true);
        // Amount of message to receive
        durableHierarchicalTopicConsumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        durableHierarchicalTopicConsumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT/10L);
        durableHierarchicalTopicConsumerConfig.setDurable(true, "diffSub3");


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration queueConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, TOPIC_NAME);
        // Use a listener
        queueConsumerConfig.setAsync(true);
        // Amount of message to receive
        queueConsumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        // Prints per message
        queueConsumerConfig.setPrintsPerMessageCount(EXPECTED_COUNT/10L);


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, TOPIC_NAME);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);


//        log.info(durableTopicConsumerConfig1.toString());
//        log.info("\n");
//        log.info(durableTopicConsumerConfig2.toString());
//        log.info("\n");
//        log.info(normalTopicConsumerConfig.toString());
//        log.info("\n");
//        log.info(normalHierarchicalTopicConsumerConfig.toString());
//        log.info("\n");
//        log.info(durableHierarchicalTopicConsumerConfig.toString());
//        log.info("\n");
//        log.info(queueConsumerConfig.toString());
//        log.info("\n");


        //Creating clients
        AndesClient durableTopicConsumerClient1 = new AndesClient(durableTopicConsumerConfig1);
        durableTopicConsumerClient1.startClient();

        AndesClient durableTopicConsumerClient2 = new AndesClient(durableTopicConsumerConfig2);
        durableTopicConsumerClient2.startClient();

        AndesClient normalTopicConsumerClient = new AndesClient(normalTopicConsumerConfig);
        normalTopicConsumerClient.startClient();

        AndesClient normalHierarchicalTopicConsumerClient = new AndesClient(normalHierarchicalTopicConsumerConfig);
        normalHierarchicalTopicConsumerClient.startClient();

        AndesClient durableHierarchicalTopicConsumerClient = new AndesClient(durableHierarchicalTopicConsumerConfig);
        durableHierarchicalTopicConsumerClient.startClient();

        AndesClient queueConsumerClient = new AndesClient(queueConsumerConfig);
        queueConsumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();


        // Evaluation
        AndesClientUtils.sleepForInterval(4000);

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(durableTopicConsumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(durableTopicConsumerClient1.getReceivedMessageCount(), EXPECTED_COUNT, "Message receive error from durable subscriber 1");

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(durableTopicConsumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(durableTopicConsumerClient2.getReceivedMessageCount(), EXPECTED_COUNT, "Message receive error from durable subscriber 2");

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(normalTopicConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(normalTopicConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receive error from normal topic subscriber");

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(normalHierarchicalTopicConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(normalHierarchicalTopicConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT,
                            "Message receive error from normal hierarchical topic subscriber");

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(durableHierarchicalTopicConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(durableHierarchicalTopicConsumerClient.getReceivedMessageCount(), EXPECTED_COUNT,
                            "Message receive error from durable hierarchical topic subscriber");

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(queueConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);
        Assert.assertEquals(queueConsumerClient.getReceivedMessageCount(), 0L,
                            "Message received from queue subscriber. This should not happen");

        AndesClientUtilsTemp.sleepForInterval(2000L);

        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT,
                            "Message send error");
//
//
//
//        AndesJMSConsumerClientConfiguration normalTopicConsumerConfig2 = (AndesJMSConsumerClientConfiguration) durableTopicConsumerConfig1.clone();
//        normalTopicConsumerConfig2.setDurable(false);
//        normalTopicConsumerConfig2.setSubscriptionID(StringUtils.EMPTY);
//
//        AndesJMSConsumerClientConfiguration normalHierarchicalTopicConsumerConfig2 = (AndesJMSConsumerClientConfiguration) durableTopicConsumerConfig1.clone();
//        normalHierarchicalTopicConsumerConfig2.setDurable(false);
//        normalHierarchicalTopicConsumerConfig2.setSubscriptionID(StringUtils.EMPTY);
//        normalHierarchicalTopicConsumerConfig2.setDestinationName(HIERARCHICAL_TOPIC);
//
//        AndesJMSConsumerClientConfiguration normalHierarchicalTopicConsumerConfig2 = (AndesJMSConsumerClientConfiguration) durableTopicConsumerConfig1.clone();
//        normalHierarchicalTopicConsumerConfig2.setDurable(false);
//        normalHierarchicalTopicConsumerConfig2.setSubscriptionID("sub3");
//        normalHierarchicalTopicConsumerConfig2.setDestinationName(HIERARCHICAL_TOPIC);
//
//        Integer sendCount = 1000;
//        Integer  AndesClientConstants.DEFAULT_RUN_TIME = 20;
//        Integer expectedCount = 500;
//
//        String topicName = "a.b.c";
//        String hierarchicalTopic = "a.b.*";
//
//        // Start durable subscription 1
//        AndesClientTemp durableTopicSub1 = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                               "topic:" + topicName,
//                                                               "100", "false",  AndesClientConstants.DEFAULT_RUN_TIME.toString(),
//                                                               expectedCount.toString(),
//                                                               "1",
//                                                               "listener=true,ackMode=1,durable=true," +
//                                                               "subscriptionID=sub1,delayBetweenMsg=0," +
//                                                               "stopAfter=" + expectedCount, "");
//        durableTopicSub1.startWorking();
//
//        // Start durable subscription 2
//        AndesClientTemp durableTopicSub2 = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                               "topic:" + topicName,
//                                                               "100", "false",  AndesClientConstants.DEFAULT_RUN_TIME.toString(),
//                                                               expectedCount.toString(),
//                                                               "1",
//                                                               "listener=true,ackMode=1,durable=true," +
//                                                               "subscriptionID=sub2,delayBetweenMsg=0," +
//                                                               "stopAfter=" + expectedCount, "");
//        durableTopicSub2.startWorking();
//
//        //start a normal topic subscriber
//        AndesClientTemp normalTopicSub = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                             "topic:" + topicName,
//                                                             "100", "false",  AndesClientConstants.DEFAULT_RUN_TIME.toString(),
//                                                             expectedCount.toString(),
//                                                             "1",
//                                                             "listener=true,ackMode=1,durable=false," +
//                                                             "delayBetweenMsg=0," +
//                                                             "stopAfter=" + expectedCount, "");
//        normalTopicSub.startWorking();
//
//        //start a hierarchical normal topic subscriber
//        AndesClientTemp normalHierarchicalTopicSub = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                                         "topic:" + hierarchicalTopic,
//                                                                         "100", "false",  AndesClientConstants.DEFAULT_RUN_TIME.toString(),
//                                                                         expectedCount.toString(),
//                                                                         "1",
//                                                                         "listener=true,ackMode=1," +
//                                                                         "durable=false," +
//                                                                         "delayBetweenMsg=0," +
//                                                                         "stopAfter=" + expectedCount, "");
//        normalHierarchicalTopicSub.startWorking();
//
//        //start a hierarchical durable topic subscriber
//        AndesClientTemp durableHierarchicalTopicSub = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                                          "topic:" + hierarchicalTopic,
//                                                                          "100", "false",
//                                                                           AndesClientConstants.DEFAULT_RUN_TIME.toString(),
//                                                                          expectedCount.toString(),
//                                                                          "1",
//                                                                          "listener=true,ackMode=1," +
//                                                                          "durable=true," +
//                                                                          "subscriptionID=sub3," +
//                                                                          "delayBetweenMsg=0," +
//                                                                          "stopAfter=" + expectedCount, "");
//        durableHierarchicalTopicSub.startWorking();
//
//        //start a queue subscriber
//        AndesClientTemp queueSubscriber = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                              "queue:" + topicName,
//                                                              "100", "false",  AndesClientConstants.DEFAULT_RUN_TIME.toString(),
//                                                              expectedCount.toString(),
//                                                              "1",
//                                                              "listener=true,ackMode=1,durable=false," +
//                                                              "delayBetweenMsg=0," +
//                                                              "stopAfter=" + expectedCount, "");
//        queueSubscriber.startWorking();
//
//
//        // Start message publisher
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672",
//                                                            "topic:" + topicName,
//                                                            "100", "false",
//                                                             AndesClientConstants.DEFAULT_RUN_TIME.toString(), sendCount.toString(), "1",
//                                                            "ackMode=1,delayBetweenMsg=0," +
//                                                            "stopAfter=" + sendCount,
//                                                            "");
//        sendingClient.startWorking();
//
//        AndesClientUtils.sleepForInterval(4000);
//
//        boolean durableTopicSub1Success = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(durableTopicSub1, expectedCount,
//                                               AndesClientConstants.DEFAULT_RUN_TIME);
//        assertTrue(durableTopicSub1Success, "Message receive error from durable subscriber 1");
//
//        boolean durableTopicSub2Success = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(durableTopicSub2, expectedCount,
//                                               AndesClientConstants.DEFAULT_RUN_TIME);
//
//        assertTrue(durableTopicSub2Success, "Message receive error from durable subscriber 2");
//
//        boolean normalTopicSubSuccess = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(normalTopicSub, expectedCount,
//                                               AndesClientConstants.DEFAULT_RUN_TIME);
//        assertTrue(normalTopicSubSuccess, "Message receive error from normal topic subscriber");
//
//        boolean normalHierarchicalTopicSubSuccess = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(normalHierarchicalTopicSub, expectedCount,
//                                               AndesClientConstants.DEFAULT_RUN_TIME);
//        assertTrue(normalHierarchicalTopicSubSuccess,
//                   "Message receive error from normal hierarchical topic subscriber");
//
//        boolean durableHierarchicalTopicSubSuccess = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(durableHierarchicalTopicSub, expectedCount,
//                                               AndesClientConstants.DEFAULT_RUN_TIME);
//        assertTrue(durableHierarchicalTopicSubSuccess,
//                   "Message receive error from durable hierarchical topic subscriber");
//
//        boolean queueSubscriberSuccess = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(queueSubscriber, expectedCount,
//                                               AndesClientConstants.DEFAULT_RUN_TIME);
//        assertFalse(queueSubscriberSuccess,
//                    "Message received from queue subscriber. This should not happen");
//        AndesClientUtilsTemp.sleepForInterval(2000);
//
//        boolean sendingSuccess = AndesClientUtilsTemp.getIfPublisherIsSuccess(sendingClient, sendCount);
//        assertTrue(sendingSuccess, "Message send error");
    }
}

