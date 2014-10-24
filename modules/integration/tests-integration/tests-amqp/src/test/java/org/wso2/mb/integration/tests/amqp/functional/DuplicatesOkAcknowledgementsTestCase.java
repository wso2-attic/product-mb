package org.wso2.mb.integration.tests.amqp.functional;


import org.testng.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

public class DuplicatesOkAcknowledgementsTestCase {


    private final Log log = LogFactory.getLog(AutoAcknowledgementsTestCase.class);

    @BeforeClass
    public void prepare() {

        AndesClientUtils.sleepForInterval(1500);
    }


    @Test(groups = {"wso2.mb", "queue"})
    public void DuplicatesOkAcknowledgementsTest() {


        Integer sendCount = 100;
        Integer runTime = 20;
        int expectedCount = 100;
        int totalMessagesReceived = 0;
        int duplicateCount;

        //Create receiving client
        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:dupOkAckTestQueue",
                "100", "false", runTime.toString(), String.valueOf(expectedCount),
                "1", "listener=true,ackMode=3,delayBetweenMsg=10,stopAfter=50", "");
        //start receiving client
        receivingClient.startWorking();
        //Create sending client
        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:dupOkAckTestQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1", "ackMode=1,delayBetweenMsg=10,stopAfter=" + sendCount, "");
        //start sending client
        sendingClient.startWorking();

        AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        totalMessagesReceived += receivingClient.getReceivedqueueMessagecount();
        //stop receiving client
        receivingClient.shutDownClient();

        AndesClientUtils.sleepForInterval(5000);

        //Create new receiving client
        AndesClient receivingClientAfterDrop = new AndesClient("receive", "127.0.0.1:5672", "queue:dupOkAckTestQueue",
                "100", "false", runTime.toString(), String.valueOf(expectedCount),
                "1", "listener=true,ackMode=3,delayBetweenMsg=10,stopAfter=500", "");

        //start new receiving client
        receivingClientAfterDrop.startWorking();

        AndesClientUtils.sleepForInterval(3000);

        AndesClientUtils.waitUntilMessagesAreReceived(receivingClientAfterDrop, expectedCount, 15);

        totalMessagesReceived += receivingClientAfterDrop.getReceivedqueueMessagecount();


        //get duplicates
        duplicateCount = receivingClientAfterDrop.getTotalNumberOfDuplicates();

        log.info("DuplicatesOkAcknowledgementsTestCase");


        Assert.assertEquals(totalMessagesReceived, (expectedCount + duplicateCount),
                "Total number of received message should be equal sum of expected and duplicate message count ");
    }
}
