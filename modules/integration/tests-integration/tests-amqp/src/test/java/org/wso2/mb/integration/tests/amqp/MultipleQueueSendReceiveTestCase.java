package org.wso2.mb.integration.tests.amqp;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;


/**
 * 1. use two queues q1, q2. 2 subscribers for q1 and one subscriber for q2
 * 2. use two publishers for q1 and one for q2
 * 3. check if messages were received correctly
 */
public class MultipleQueueSendReceiveTestCase {

    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performMultipleQueueSendReceiveTestCase() {

        Integer sendCount = 2000;
        Integer runTime = 20;
        int additional = 10;

        //wait some more time to see if more messages are received
        Integer expectedCount = 2000 + additional;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:multipleQueue1,multipleQueue2,", "100", "false",
                runTime.toString(), expectedCount.toString(), "3",
                "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:multipleQueue1,multipleQueue2", "100",
                "false", runTime.toString(), sendCount.toString(), "3",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean success = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean receiveSuccess = false;
        if((expectedCount - additional) == receivingClient.getReceivedqueueMessagecount()) {
            receiveSuccess = true;
        }

        if(receiveSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals(receiveSuccess, true);
    }
}
