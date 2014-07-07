package org.wso2.mb.integration.tests.amqp;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

/**
 * 1. use two topics t1, t2. 2 subscribers for t1 and one subscriber for t2
 * 2. use two publishers for t1 and one for t2
 * 3. check if messages were received correctly
 */
public class MultipleTopicPublishSubscribeTestCase {

    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "topic"})
    public void performMultipleTopicPublishSubscribeTestCase() {

        Integer sendCount1 = 1000;
        Integer sendCount2 = 2000;
        Integer runTime = 40;
        int additional = 10;

        //expect little more to check if no more messages are received
        Integer expectedCount2 = 4000 + additional;
        Integer expectedCount1 = 1000 + additional;

        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672", "topic:multipleTopic2,", "100", "false",
                runTime.toString(), expectedCount2.toString(), "2",
                "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount2, "");

        AndesClient receivingClient1 = new AndesClient("receive", "127.0.0.1:5672", "topic:multipleTopic1,", "100", "false",
                runTime.toString(), expectedCount1.toString(), "1",
                "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount1, "");

        receivingClient1.startWorking();
        receivingClient2.startWorking();


        AndesClient sendingClient2 = new AndesClient("send", "127.0.0.1:5672", "topic:multipleTopic2", "100",
                "false", runTime.toString(), sendCount2.toString(), "2",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount2, "");

        AndesClient sendingClient1 = new AndesClient("send", "127.0.0.1:5672", "topic:multipleTopic1", "100",
                "false", runTime.toString(), sendCount1.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount1, "");

        sendingClient1.startWorking();
        sendingClient2.startWorking();

        boolean success1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1, expectedCount1, runTime);
        boolean success2 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2, expectedCount2, runTime);

        boolean receiveSuccess1 = false;
        if((expectedCount1 - additional) == receivingClient1.getReceivedTopicMessagecount()) {
            receiveSuccess1 = true;
        }
        boolean receiveSuccess2 =false;
        if((expectedCount2 - additional) == receivingClient2.getReceivedTopicMessagecount()) {
            receiveSuccess2 = true;
        }

        if(receiveSuccess1 && receiveSuccess2) {
            System.out.println("TEST PASSED");
        } else  {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals(receiveSuccess1 && receiveSuccess2, true);
    }
}
