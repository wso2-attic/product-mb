package org.wso2.mb.integration.tests.amqp.functional;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

public class TransactedAcknowledgementsTestCase {

    private final Log log = LogFactory.getLog(TransactedAcknowledgementsTestCase.class);

    @BeforeClass
    public void prepare() {

        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void TransactedAcknowledgements() {
        Integer sendCount = 10;
        Integer runTime = 20;
        int expectedCount = 10;
        int totalMessagesReceived = 0;
        int duplicateCount;

        boolean transactedOperationStatus;
        boolean success;

        String transactedOperationMessage = "Unsuccessful";
        AndesClient receivingClient;
        AndesClient sendingClient;


        //Create  receiving client
        receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:transactedAckTestQueue",
                "100", "true", runTime.toString(), String.valueOf(expectedCount),
                "1", "listener=true,ackMode=0,delayBetweenMsg=100,stopAfter=100,rollbackAfterEach=10,commitAfterEach=30", "");
        //start receiving client
        receivingClient.startWorking();

        //Create  sending client
        sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:transactedAckTestQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1", "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
        //start sending client
        sendingClient.startWorking();


        success = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        totalMessagesReceived += receivingClient.getReceivedqueueMessagecount();
        //if received messages less than expected number wait until received again

        //get rollback status , check message id of next message of roll backed message equal to first message
        transactedOperationStatus = receivingClient.transactedOperation(10);
        duplicateCount = receivingClient.getTotalNumberOfDuplicates();

        if (!success) {
            AndesClientUtils.sleepForInterval(3000);

            receivingClient.startWorking();
            AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, 15);

            totalMessagesReceived += receivingClient.getReceivedqueueMessagecount();

        }

        log.info("TransactedAcknowledgementsTestCase");


        Assert.assertEquals(totalMessagesReceived, (expectedCount + duplicateCount),
                "Total number of received message should be equal sum of expected and duplicate message count ");

        Assert.assertTrue(transactedOperationStatus, "After rollback next message need to equal first message of batch");


    }


}
