package org.wso2.mb.integration.tests.amqp.functional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TemporaryTopicSubscriptionVerificationTestCase {

    private static Log log = LogFactory.getLog(TemporaryTopicSubscriptionVerificationTestCase.class);

    @Test(groups = {"wso2.mb", "topic"},
            description = "Single topic subscriber subscribe-close-re-subscribe test case")
    public void performSingleTopicSubscribeCloseResubscribeTest()
            throws AndesClientException, NamingException, JMSException, IOException {


        long sendCount = 1000L;
        long expectedCount = 200L;

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration initialConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "singleSubscribeAndCloseTopic");
        // Amount of message to receive
        initialConsumerConfig.setMaximumMessagesToReceived(expectedCount);
        // Prints per message
        initialConsumerConfig.setPrintsPerMessageCount(expectedCount / 10L);


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "singleSubscribeAndCloseTopic");
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);
        publisherConfig.setNumberOfMessagesToSend(sendCount);


        AndesClient initialConsumerClient = new AndesClient(initialConsumerConfig);
        initialConsumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(initialConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);


        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration secondaryConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "singleSubscribeAndCloseTopic");
        // Amount of message to receive
        secondaryConsumerConfig.setMaximumMessagesToReceived(1L);

        AndesClient secondaryConsumerClient = new AndesClient(secondaryConsumerConfig);
        secondaryConsumerClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(secondaryConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);


        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message send failed");

        Assert.assertEquals(initialConsumerClient.getReceivedMessageCount(), expectedCount, "Initial consumer failed to receive messages");

        Assert.assertEquals(secondaryConsumerClient.getReceivedMessageCount(), 0, "Message received after re-subscribing for a temporary topic.");


//
//
//
//
//        Integer sendCount = 1000;
//        Integer runTime = 20;
//        Integer expectedCount = 200;
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                      "topic:singleSubscribeAndCloseTopic",
//                                                      "100", "false", runTime.toString(),
//                                                      expectedCount.toString(),
//                                                      "1",
//                                                      "listener=true,ackMode=1,delayBetweenMsg=0," +
//                                                      "stopAfter=" + expectedCount,
//                                                      "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:singleSubscribeAndCloseTopic",
//                                                    "100", "false",
//                                                    runTime.toString(), sendCount.toString(), "1",
//                                                    "ackMode=1,delayBetweenMsg=0," +
//                                                    "stopAfter=" + sendCount,
//                                                    "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(sendSuccess, "Message sending failed.");
//
//        Assert.assertTrue(receiveSuccess, "Message receiving failed.");
//
//        //re-subscribe and see if messages are coming
//        AndesClientTemp newReceivingClient = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                         "topic:singleSubscribeAndCloseTopic",
//                                                         "100", "false", runTime.toString(), "1",
//                                                         "1",
//                                                         "listener=true,ackMode=1," +
//                                                         "delayBetweenMsg=0,stopAfter=" + "1",
//                                                         "");
//        newReceivingClient.startWorking();
//
//        boolean messagesAreReceived = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(newReceivingClient, 1, runTime);
//
//        Assert.assertFalse(messagesAreReceived,
//                           "Message received after re-subscribing for a temporary topic.");
    }

    /**
     * 1. put a topic subscriber
     * 2. put another topic subscriber. It will receive some of the messages and close
     * 3. resubscribe to same topic and see if messages are received. (when first subscriber is still getting messages)
     *
     * @throws ExecutionException
     */
    @Test(groups = {"wso2.mb", "topic"}, description = "Single topic subscriber subscribe-close-" +
                                                       "re-subscribe test case with multiple " +
                                                       "subscriptions")
    public void performMultipleTopicSubscribeCloseResubscribeTest()
            throws ExecutionException, AndesClientException, NamingException, JMSException,
                   IOException {

        long sendCount = 1000L;
        long expectedCountByClient1 = 1000L;
        long expectedCountByClient2 = 200L;

        ExecutorService service = Executors.newSingleThreadExecutor();

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration initialConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "multiSubscribeAndCloseTopic");
        // Amount of message to receive
        initialConsumerConfig.setMaximumMessagesToReceived(expectedCountByClient1);
        // Prints per message
        initialConsumerConfig.setPrintsPerMessageCount(expectedCountByClient1 / 10L);
        initialConsumerConfig.setRunningDelay(100L);

// Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration secondaryConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "multiSubscribeAndCloseTopic");
        // Amount of message to receive
        secondaryConsumerConfig.setMaximumMessagesToReceived(expectedCountByClient2);
        // Prints per message
        secondaryConsumerConfig.setPrintsPerMessageCount(expectedCountByClient2 / 10L);
        secondaryConsumerConfig.setRunningDelay(100L);


        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, "multiSubscribeAndCloseTopic");
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);
        publisherConfig.setNumberOfMessagesToSend(sendCount);


        AndesClient initialConsumerClient = new AndesClient(initialConsumerConfig);
        initialConsumerClient.startClient();

        AndesClient secondaryConsumerClient = new AndesClient(secondaryConsumerConfig);
        secondaryConsumerClient.startClient();

        //schedule another subscriber to run after 'first client is closed'
        ConcurrentReceiverTask receiverTask = new ConcurrentReceiverTask();
        Future<Boolean> future = service.submit(receiverTask);
        ;


        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();



        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(initialConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(secondaryConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);



        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message send failed");

        Assert.assertEquals(initialConsumerClient.getReceivedMessageCount(), expectedCountByClient1, "Message receiving failed by client 1.");
        Assert.assertEquals(secondaryConsumerClient.getReceivedMessageCount(), expectedCountByClient2, "Message receiving failed by client 2.");


        try {
            Boolean newReceivingClientResult = future.get();
            Assert.assertFalse(newReceivingClientResult,
                               "Message received after re-subscribing for a temporary topic when" +
                               " another subscription to same topic is around.");
        } catch (InterruptedException e) {
            //ignore
        }

        
        








//        Integer sendCount = 1000;
//        Integer runTimeForClient1 = 150;
//        Integer runTimeForClient2 = 20;
//        Integer runTimeForSendClient = 20;
//        Integer expectedCountByClient1 = 1000;
//        Integer expectedCountByClient2 = 200;
//
//        ExecutorService service = Executors.newSingleThreadExecutor();
//
//        //start a receiver with a delay
//        AndesClientTemp receivingClient1 = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                               "topic:multiSubscribeAndCloseTopic",
//                                                               "100", "false", runTimeForClient1.toString(),
//                                                               expectedCountByClient1.toString(),
//                                                               "1",
//                                                               "listener=true,ackMode=1," +
//                                                               "delayBetweenMsg=100," +
//                                                               "stopAfter=" + expectedCountByClient1,
//                                                               "");
//
//        //start another receiver with same params. This will receive some of the messages and close
//        AndesClientTemp receivingClient2 = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                               "topic:multiSubscribeAndCloseTopic",
//                                                               "100", "false", runTimeForClient2.toString(),
//                                                               expectedCountByClient2.toString(),
//                                                               "1",
//                                                               "listener=true,ackMode=1," +
//                                                               "delayBetweenMsg=100," +
//                                                               "stopAfter=" + expectedCountByClient2,
//                                                               "");
//
//        receivingClient1.startWorking();
//        receivingClient2.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:multiSubscribeAndCloseTopic",
//                                                            "100", "false",
//                                                            runTimeForSendClient.toString(), sendCount.toString(), "1",
//                                                            "ackMode=1,delayBetweenMsg=0," +
//                                                            "stopAfter=" + sendCount,
//                                                            "");
//
//        //schedule another subscriber to run after 'first client is closed'
//        ConcurrentReceiverTask receiverTask = new ConcurrentReceiverTask();
//        Future<Boolean> future = service.submit(receiverTask);
//        Boolean newReceivingClientResult = null;
//
//
//        sendingClient.startWorking();
//
//        boolean receiver1Success = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(receivingClient1, expectedCountByClient1, runTimeForClient1);
//
//        boolean receiver2Success = AndesClientUtilsTemp
//                .waitUntilMessagesAreReceived(receivingClient2, expectedCountByClient2, runTimeForClient2);
//
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(sendSuccess, "Message sending failed.");
//
//        Assert.assertTrue(receiver1Success, "Message receiving failed by client 1.");
//
//        Assert.assertTrue(receiver2Success, "Message receiving failed by client 2.");
//
//        try {
//            newReceivingClientResult = future.get();
//        } catch (InterruptedException e) {
//            //ignore
//        }
//        Assert.assertFalse(newReceivingClientResult,
//                           "Message received after re-subscribing for a temporary topic when" +
//                           " another subscription to same topic is around.");
    }

    private class ConcurrentReceiverTask implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            //wait
            TimeUnit.SECONDS.sleep(30);
            //re-subscribe and see if messages are coming
            // Creating a initial JMS consumer client configuration
            AndesJMSConsumerClientConfiguration newConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "multiSubscribeAndCloseTopic");
            // Amount of message to receive
            newConsumerConfig.setMaximumMessagesToReceived(1L);
            // Prints per message


            AndesClient newConsumerClient = new AndesClient(newConsumerConfig);
            newConsumerClient.startClient();


            AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(newConsumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

            return newConsumerClient.getReceivedMessageCount() == 1L;


//            AndesClientTemp newReceivingClient = new AndesClientTemp("receive", "127.0.0.1:5672",
//                                                                     "topic:multiSubscribeAndCloseTopic",
//                                                                     "1", "false", runTime.toString(), "1",
//                                                                     "1",
//                                                                     "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + "1",
//                                                                     "");
//            newReceivingClient.startWorking();
//
//            boolean messagesAreReceived = AndesClientUtilsTemp
//                    .waitUntilMessagesAreReceived(newReceivingClient, 1, runTime);
//            return messagesAreReceived;
        }
    }
}
