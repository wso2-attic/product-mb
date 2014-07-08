package org.wso2.mb.integration.tests.amqp;

import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import static org.testng.Assert.assertTrue;

/**
 * 1. start two durable topic subscription
 * 2. send 1500 messages
 */
public class DurableMultipleTopicSubscriberTestCase {

    @Test(groups={"wso2.mb", "durableTopic"})
    public void performDurableTopicTestCase() {

        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 500;

        // Start subscription 1
        AndesClient receivingClient1 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0,stopAfter="+expectedCount, "");
        receivingClient1.startWorking();

        // Start subscription 2
        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub2,delayBetweenMsg=0,stopAfter="+expectedCount, "");
        receivingClient2.startWorking();

        // Start message publisher
        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");
        sendingClient.startWorking();

        AndesClientUtils.sleepForInterval(2000);

        boolean receivingSuccess1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1, expectedCount , runTime);
        assertTrue(receivingSuccess1,"Message receive error from subscriber 1");

        boolean receivingSuccess2 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2, expectedCount , runTime);
        assertTrue(receivingSuccess2,"Message receive error from subscriber 2");

        AndesClientUtils.sleepForInterval(2000);

        boolean sendingSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);
        assertTrue(sendingSuccess,"Message send error");
    }
}
