package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * This test class with perform test case of having different types of subscriptions together with
 * durable topic subscription.
 */
public class DifferentSubscriptionsWithDurableTopicTestCase {

    @Test(groups = {"wso2.mb", "durableTopic"})
    public void performDifferentTopicSubscriptionsWithDurableTopicTest() {

        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 500;

        String topicName = "a.b.c";
        String hierarchicalTopic = "a.b.*";

        // Start durable subscription 1
        AndesClient durableTopicSub1 = new AndesClient("receive", "127.0.0.1:5672",
                                                       "topic:" + topicName,
                                                       "100", "false", runTime.toString(),
                                                       expectedCount.toString(),
                                                       "1",
                                                       "listener=true,ackMode=1,durable=true," +
                                                       "subscriptionID=sub21,delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount, "");
        durableTopicSub1.startWorking();

        // Start durable subscription 2
        AndesClient durableTopicSub2 = new AndesClient("receive", "127.0.0.1:5672",
                                                       "topic:" + topicName,
                                                       "100", "false", runTime.toString(),
                                                       expectedCount.toString(),
                                                       "1",
                                                       "listener=true,ackMode=1,durable=true," +
                                                       "subscriptionID=sub22,delayBetweenMsg=0," +
                                                       "stopAfter=" + expectedCount, "");
        durableTopicSub2.startWorking();

        //start a normal topic subscriber
        AndesClient normalTopicSub = new AndesClient("receive", "127.0.0.1:5672",
                                                     "topic:" + topicName,
                                                     "100", "false", runTime.toString(),
                                                     expectedCount.toString(),
                                                     "1",
                                                     "listener=true,ackMode=1,durable=false," +
                                                     "delayBetweenMsg=0," +
                                                     "stopAfter=" + expectedCount, "");
        normalTopicSub.startWorking();

        //start a hierarchical normal topic subscriber
        AndesClient normalHierarchicalTopicSub = new AndesClient("receive", "127.0.0.1:5672",
                                                                 "topic:" + hierarchicalTopic,
                                                                 "100", "false", runTime.toString(),
                                                                 expectedCount.toString(),
                                                                 "1",
                                                                 "listener=true,ackMode=1," +
                                                                 "durable=false," +
                                                                 "delayBetweenMsg=0," +
                                                                 "stopAfter=" + expectedCount, "");
        normalHierarchicalTopicSub.startWorking();

        //start a hierarchical durable topic subscriber
        AndesClient durableHierarchicalTopicSub = new AndesClient("receive", "127.0.0.1:5672",
                                                                  "topic:" + hierarchicalTopic,
                                                                  "100", "false",
                                                                  runTime.toString(),
                                                                  expectedCount.toString(),
                                                                  "1",
                                                                  "listener=true,ackMode=1," +
                                                                  "durable=true," +
                                                                  "subscriptionID=sub23," +
                                                                  "delayBetweenMsg=0," +
                                                                  "stopAfter=" + expectedCount, "");
        durableHierarchicalTopicSub.startWorking();

        //start a queue subscriber
        AndesClient queueSubscriber = new AndesClient("receive", "127.0.0.1:5672",
                                                      "queue:" + topicName,
                                                      "100", "false", runTime.toString(),
                                                      expectedCount.toString(),
                                                      "1",
                                                      "listener=true,ackMode=1,durable=false," +
                                                      "delayBetweenMsg=0," +
                                                      "stopAfter=" + expectedCount, "");
        queueSubscriber.startWorking();


        // Start message publisher
        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672",
                                                    "topic:" + topicName,
                                                    "100", "false",
                                                    runTime.toString(), sendCount.toString(), "1",
                                                    "ackMode=1,delayBetweenMsg=0," +
                                                    "stopAfter=" + sendCount,
                                                    "");
        sendingClient.startWorking();

        AndesClientUtils.sleepForInterval(4000);

        boolean durableTopicSub1Success = AndesClientUtils
                .waitUntilMessagesAreReceived(durableTopicSub1, expectedCount,
                                              runTime);
        assertTrue(durableTopicSub1Success, "Message receive error from durable subscriber 1");

        boolean durableTopicSub2Success = AndesClientUtils
                .waitUntilMessagesAreReceived(durableTopicSub2, expectedCount,
                                              runTime);

        assertTrue(durableTopicSub2Success, "Message receive error from durable subscriber 2");

        boolean normalTopicSubSuccess = AndesClientUtils
                .waitUntilMessagesAreReceived(normalTopicSub, expectedCount,
                                              runTime);
        assertTrue(normalTopicSubSuccess, "Message receive error from normal topic subscriber");

        boolean normalHierarchicalTopicSubSuccess = AndesClientUtils
                .waitUntilMessagesAreReceived(normalHierarchicalTopicSub, expectedCount,
                                              runTime);
        assertTrue(normalHierarchicalTopicSubSuccess,
                   "Message receive error from normal hierarchical topic subscriber");

        boolean durableHierarchicalTopicSubSuccess = AndesClientUtils
                .waitUntilMessagesAreReceived(durableHierarchicalTopicSub, expectedCount,
                                              runTime);
        assertTrue(durableHierarchicalTopicSubSuccess,
                   "Message receive error from durable hierarchical topic subscriber");

        boolean queueSubscriberSuccess = AndesClientUtils
                .waitUntilMessagesAreReceived(queueSubscriber, expectedCount,
                                              runTime);
        assertFalse(queueSubscriberSuccess,
                    "Message received from queue subscriber. This should not happen");
        AndesClientUtils.sleepForInterval(2000);

        boolean sendingSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);
        assertTrue(sendingSuccess, "Message send error");
    }
}

