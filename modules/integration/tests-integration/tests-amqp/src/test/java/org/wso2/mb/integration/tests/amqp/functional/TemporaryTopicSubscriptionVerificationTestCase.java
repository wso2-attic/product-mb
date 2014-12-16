package org.wso2.mb.integration.tests.amqp.functional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TemporaryTopicSubscriptionVerificationTestCase {

    private static Log log = LogFactory.getLog(TemporaryTopicSubscriptionVerificationTestCase.class);

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

    /**
     * 1. put a topic subscriber
     * 2. put another topic subscriber. It will receive some of the messages and close
     * 3. resubscribe to same topic and see if messages are received. (when first subscriber is still getting messages)
     * @throws ExecutionException
     */
    @Test(groups = {"wso2.mb", "topic"}, description = "Single topic subscriber subscribe-close-" +
                                                       "re-subscribe test case with multiple " +
                                                       "subscriptions")
    public void performMultipleTopicSubscribeCloseResubscribeTest() throws ExecutionException {
        Integer sendCount = 1000;
        Integer runTimeForClient1 = 150;
        Integer runTimeForClient2 = 20;
        Integer runTimeForSendClient = 20;
        Integer expectedCountByClient1 = 1000;
        Integer expectedCountByClient2 = 200;

        ExecutorService service =  Executors.newSingleThreadExecutor();

        //start a receiver with a delay
        AndesClient receivingClient1 = new AndesClient("receive", "127.0.0.1:5672",
                                                       "topic:multiSubscribeAndCloseTopic",
                                                       "100", "false", runTimeForClient1.toString(),
                                                       expectedCountByClient1.toString(),
                                                       "1",
                                                       "listener=true,ackMode=1," +
                                                       "delayBetweenMsg=100," +
                                                       "stopAfter=" + expectedCountByClient1,
                                                       "");

        //start another receiver with same params. This will receive some of the messages and close
        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672",
                                                       "topic:multiSubscribeAndCloseTopic",
                                                       "100", "false", runTimeForClient2.toString(),
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
                                                    runTimeForSendClient.toString(), sendCount.toString(), "1",
                                                    "ackMode=1,delayBetweenMsg=0," +
                                                    "stopAfter=" + sendCount,
                                                    "");

        //schedule another subscriber to run after 'first client is closed'
        ConcurrentReceiverTask receiverTask = new ConcurrentReceiverTask();
        Future<Boolean> future = service.submit(receiverTask);
        Boolean newReceivingClientResult = null;


        sendingClient.startWorking();

        boolean receiver1Success = AndesClientUtils
                .waitUntilMessagesAreReceived(receivingClient1, expectedCountByClient1, runTimeForClient1);

        boolean receiver2Success = AndesClientUtils
                .waitUntilMessagesAreReceived(receivingClient2, expectedCountByClient2, runTimeForClient2);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        Assert.assertTrue(sendSuccess, "Message sending failed.");

        Assert.assertTrue(receiver1Success, "Message receiving failed by client 1.");

        Assert.assertTrue(receiver2Success, "Message receiving failed by client 2.");

        try {
            newReceivingClientResult = future.get();
        } catch (InterruptedException e) {
            //ignore
        }
        Assert.assertFalse(newReceivingClientResult,
                           "Message received after re-subscribing for a temporary topic when" +
                           " another subscription to same topic is around.");
    }

    private class ConcurrentReceiverTask implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            //wait
            log.info("====Async thread launched...");
            Thread.sleep(30 * 1000);
            log.info("====Async subscriber subscribing...");
            Integer runTime = 20;
            //re-subscribe and see if messages are coming
            AndesClient newReceivingClient = new AndesClient("receive", "127.0.0.1:5672",
                                                             "topic:multiSubscribeAndCloseTopic",
                                                             "1", "false", runTime.toString(), "1",
                                                             "1",
                                                             "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + "1",
                                                             "");
            newReceivingClient.startWorking();

            boolean messagesAreReceived = AndesClientUtils
                    .waitUntilMessagesAreReceived(newReceivingClient, 1, runTime);
            System.out.println("Retuning result = " + messagesAreReceived);
            return messagesAreReceived;
        }
    }
}
