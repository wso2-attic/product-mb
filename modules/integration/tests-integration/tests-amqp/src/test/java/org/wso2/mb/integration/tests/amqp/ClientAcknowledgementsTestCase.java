package org.wso2.mb.integration.tests.amqp;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

/**
 * 1. start a queue receiver in client ack mode
 * 2. receive messages acking message bunch to bunch
 * 3. after all messages are received subscribe again and verify n more messages come
 */
public class ClientAcknowledgementsTestCase {

    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups={"wso2.mb", "queue"})
    public void performClientAcknowledgementsTestCase() {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;
        Integer totalMsgsReceived = 0;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:clientAckTestQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=2,delayBetweenMsg=0,ackAfterEach=200,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:clientAckTestQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1","ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean success = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        totalMsgsReceived +=  receivingClient.getReceivedqueueMessagecount();

        AndesClientUtils.sleepForInterval(2000);

        receivingClient.startWorking();
        AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, 15);

        totalMsgsReceived +=  receivingClient.getReceivedqueueMessagecount();

        if(totalMsgsReceived.equals(expectedCount)) {
            System.out.println("TEST PASSED");
        } else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals(expectedCount,totalMsgsReceived);
    }

}
