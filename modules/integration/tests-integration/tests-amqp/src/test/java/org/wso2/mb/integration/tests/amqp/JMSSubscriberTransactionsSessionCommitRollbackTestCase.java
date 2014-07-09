package org.wso2.mb.integration.tests.amqp;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import java.util.Map;

/**
 * 1. start a queue receiver with trasacted sessions
 * 2. send 10 messages
 * 3. after 10 messages are received rollback session
 * 4. do same 5 times.After 50 messages received commit the session and close subscriber
 * 5. analyse and see if each message is duplicated five times
 * 6. do another subscription to verify no more messages are received
 */
public class JMSSubscriberTransactionsSessionCommitRollbackTestCase {


    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue", "transactions"})
    public void performJMSSubscriberTransactionsSessionCommitRollbackTestCase() {

        Integer sendCount = 10;
        Integer runTime = 40;
        int numberOfRollbackIterations = 5;
        Integer expectedCount = sendCount * numberOfRollbackIterations;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:transactionQueue",
                "10", "true", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=0,delayBetweenMsg=0,rollbackAfterEach="+sendCount+",commitAfterEach="+expectedCount+",stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:transactionQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient,expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        AndesClientUtils.sleepForInterval(1000);

        Map<Long, Integer> duplicateMessages = receivingClient.checkIfMessagesAreDuplicated();

        boolean expectedCountDelivered = false;
        if(duplicateMessages != null) {
            for(Long messaggeIdentifier : duplicateMessages.keySet()) {
                int numberOfTimesDelivered = duplicateMessages.get(messaggeIdentifier);
                if(numberOfRollbackIterations == numberOfTimesDelivered) {
                    expectedCountDelivered = true;
                } else {
                    expectedCountDelivered = false;
                    break;
                }
            }
        }

        //verify no more messages are delivered

        AndesClientUtils.sleepForInterval(2000);

        receivingClient.startWorking();

        boolean areMessagesReceivedAfterwars = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient,
                expectedCount, 20);

        if(sendSuccess && receiveSuccess && expectedCountDelivered && !areMessagesReceivedAfterwars) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED :" + sendSuccess + receiveSuccess + expectedCountDelivered + !areMessagesReceivedAfterwars);
        }

        Assert.assertEquals(sendSuccess && receiveSuccess && expectedCountDelivered && !areMessagesReceivedAfterwars, true);
    }

}
