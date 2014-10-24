package org.wso2.mb.integration.tests.amqp.functional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;


public class AutoAcknowledgementsTestCase {

    private final Log log = LogFactory.getLog(AutoAcknowledgementsTestCase.class);

    @BeforeClass
    public void prepare() {

        AndesClientUtils.sleepForInterval(1500);
    }


    @Test(groups = {"wso2.mb", "queue"})
    public void AutoAcknowledgementsTestCase() {


        Integer sendCount = 1500;
        Integer runTime = 10;
        Integer expectedCount = 1500;
        Integer totalMessagesReceived = 0;
        boolean success;

        //Create receiving client
        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:autoAckTestQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=10,stopAfter=" + expectedCount, "");

        //start receiving client
        receivingClient.startWorking();

        //Create sending client
        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:autoAckTestQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1", "ackMode=1,delayBetweenMsg=10,stopAfter=" + sendCount, "");

        //start sending client
        sendingClient.startWorking();

        success = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        totalMessagesReceived += receivingClient.getReceivedqueueMessagecount();
        AndesClientUtils.sleepForInterval(2000);

        //wait again and get count if received messages less than expected number of messages
        if (!success) {
            AndesClientUtils.sleepForInterval(2000);

            receivingClient.startWorking();
            AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, 15);

            totalMessagesReceived += receivingClient.getReceivedqueueMessagecount();
        }

        log.info("AutoAcknowledgementsTestCase-Stable receiving client");

        Assert.assertEquals(totalMessagesReceived, expectedCount, "Total number of received messages");

    }

    /**
     * q
     * In this method we drop receiving client and connect it again and tries to get messages from MB
     */
    @Test(groups = {"wso2.mb", "queue"})
    public void AutoAcknowledgementsDropReceiverTestCase() {


        Integer sendCount = 1500;
        Integer runTime = 10;
        Integer expectedCount = 1500;

        //Create receiving client
        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:autoAckTestQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=10,stopAfter=1000", "");
        //start receiving client
        receivingClient.startWorking();
        //Create sending client
        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:autoAckTestQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1", "ackMode=1,delayBetweenMsg=10,stopAfter=" + sendCount, "");
        //start sending client
        sendingClient.startWorking();
        //wait until messages receive
        AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        Integer totalMessagesReceived = receivingClient.getReceivedqueueMessagecount();
        AndesClientUtils.sleepForInterval(5000);
        receivingClient.shutDownClient();

        //stop receiving client
        receivingClient.shutDownClient();

        AndesClientUtils.sleepForInterval(5000);

        //Create new receiving client
        AndesClient receivingClientAfterDrop = new AndesClient("receive", "127.0.0.1:5672", "queue:autoAckTestQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=10,stopAfter=2000", "");

        //start new receiving client
        receivingClientAfterDrop.startWorking();

        AndesClientUtils.sleepForInterval(3000);

        //wait until messages receive
        AndesClientUtils.waitUntilMessagesAreReceived(receivingClientAfterDrop, expectedCount, 15);

        totalMessagesReceived += receivingClientAfterDrop.getReceivedqueueMessagecount();
        log.info("AutoAcknowledgementsTestCase-Drop Receiving client");


        //To pass this test received number of messages equals to sent messages

        Assert.assertEquals(totalMessagesReceived, expectedCount, "Total number of received messages should be equal " +
                "to total number of sent messages");
    }

}
