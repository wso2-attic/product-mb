package org.wso2.mb.integration.tests.amqp;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

/**
 * 1. subscribe to a single queue using Client Ack
 * 2. this subscriber will wait a long time for messages (defaultAckWaitTimeout*defaultMaxRedeliveryAttempts)
 * 3. subscriber will never ack for messages
 * 4. subscriber will receive same message until defaultMaxRedeliveryAttempts breached
 * 5. after that message will be written to dlc
 * 6. no more message should be delivered after written to DLC
 */
public class QueueMessageRedeliveryWithAckTimeOutTestCase {

    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performQueueMessageRedeliveryWithAckTimeOutTestCase() {

        int defaultMaxRedeliveryAttempts = 10;
        int defaultAckWaitTimeout = 10;
        Integer sendCount = 2;

        //wait until messages go to DLC and some more time to verify no more messages are coming
        Integer expectedCount = defaultMaxRedeliveryAttempts * sendCount + 100;

        //wait until messages go to DLC and some more time to verify no more messages are coming
        Integer runTime = defaultAckWaitTimeout*defaultMaxRedeliveryAttempts + 200;

        //set AckwaitTimeout
        System.setProperty("AndesAckWaitTimeOut",Integer.toString(defaultAckWaitTimeout*1000));
        //expect 1000 messages to stop it from stopping
        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:redeliveryQueue",
                "100", "true", runTime.toString(), expectedCount.toString(),
                "2", "listener=true,ackMode=2,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:redeliveryQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean success = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        boolean receiveSuccess =  receivingClient.getReceivedqueueMessagecount() == sendCount*defaultMaxRedeliveryAttempts;

        if(sendSuccess && receiveSuccess) {
            System.out.println("TEST PASSED");
        } else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals(sendSuccess && receiveSuccess, true);
    }
}
