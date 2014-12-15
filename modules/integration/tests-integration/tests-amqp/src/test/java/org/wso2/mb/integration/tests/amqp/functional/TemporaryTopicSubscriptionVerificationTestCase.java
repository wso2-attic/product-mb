package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

public class TemporaryTopicSubscriptionVerificationTestCase {


    @Test(groups = {"wso2.mb", "topic"},
          description = "Single topic subscriber subscribe-close-re-subscribe test case")
    public void performSingleTopicSubscribeCloseResubscribeTest() {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 200;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672",
                                                      "topic:singleSubscribeAndCloseTopic",
                                                      "100", "false", runTime.toString(),
                                                      expectedCount.toString(),
                                                      "1",
                                                      "listener=true,ackMode=1,delayBetweenMsg=0," +
                                                      "stopAfter=" + expectedCount,
                                                      "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:singleSubscribeAndCloseTopic",
                                                    "100", "false",
                                                    runTime.toString(), sendCount.toString(), "1",
                                                    "ackMode=1,delayBetweenMsg=0," +
                                                    "stopAfter=" + sendCount,
                                                    "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils
                .waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Assert.assertTrue(sendSuccess, "Message sending failed.");

        Assert.assertTrue(receiveSuccess, "Message receiving failed.");

        //re-subscribe and see if messages are coming
        AndesClient newReceivingClient = new AndesClient("receive", "127.0.0.1:5672",
                                                         "topic:singleSubscribeAndCloseTopic",
                                                         "100", "false", runTime.toString(), "1",
                                                         "1",
                                                         "listener=true,ackMode=1," +
                                                         "delayBetweenMsg=0,stopAfter=" + "1",
                                                         "");
        newReceivingClient.startWorking();

        boolean messagesAreReceived = AndesClientUtils
                .waitUntilMessagesAreReceived(newReceivingClient, 1, runTime);

        Assert.assertFalse(messagesAreReceived,
                           "Message received after re-subscribing for a temporary topic.");
    }

    @Test(groups = {"wso2.mb", "topic"}, description = "Single topic subscriber subscribe-close-" +
                                                       "re-subscribe test case with multiple " +
                                                       "subscriptions")
    public void performMultipleTopicSubscribeCloseResubscribeTest() {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCountByClient1 = 1000;
        Integer expectedCountByClient2 = 200;

        //start a receiver with a delay
        AndesClient receivingClient1 = new AndesClient("receive", "127.0.0.1:5672",
                                                       "topic:multiSubscribeAndCloseTopic",
                                                       "100", "false", "150",
                                                       expectedCountByClient1.toString(),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=100," +
                                                       "stopAfter=" + expectedCountByClient1,
                                                       "");

        //start another receiver with same params. This will receive some of the messages and close
        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672",
                                                       "topic:multiSubscribeAndCloseTopic",
                                                       "100", "false", runTime.toString(),
                                                       expectedCountByClient2.toString(),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=100," +
                                                       "stopAfter=" + expectedCountByClient2,
                                                       "");

        receivingClient1.startWorking();
        receivingClient2.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:multiSubscribeAndCloseTopic",
                                                    "100", "false",
                                                    runTime.toString(), sendCount.toString(), "1",
                                                    "ackMode=1,delayBetweenMsg=0," +
                                                    "stopAfter=" + sendCount,
                                                    "");

        sendingClient.startWorking();

        boolean receiver1Success = AndesClientUtils
                .waitUntilMessagesAreReceived(receivingClient1, expectedCountByClient1, runTime);

        boolean receiver2Success = AndesClientUtils
                .waitUntilMessagesAreReceived(receivingClient2, expectedCountByClient2, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Assert.assertTrue(sendSuccess, "Message sending failed.");

        Assert.assertTrue(receiver1Success, "Message receiving failed by client 1.");

        Assert.assertTrue(receiver2Success, "Message receiving failed by client 2.");

        //re-subscribe and see if messages are coming
        AndesClient newReceivingClient = new AndesClient("receive", "127.0.0.1:5672",
                                                         "topic:multiSubscribeAndCloseTopic",
                                                         "100", "false", runTime.toString(), "1",
                                                         "1",
                                                         "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + "1",
                                                         "");
        newReceivingClient.startWorking();

        boolean messagesAreReceived = AndesClientUtils
                .waitUntilMessagesAreReceived(newReceivingClient, 1, runTime);

        Assert.assertFalse(messagesAreReceived,
                           "Message received after re-subscribing for a temporary topic when" +
                           " another subscription to same topic is around.");
    }
}
