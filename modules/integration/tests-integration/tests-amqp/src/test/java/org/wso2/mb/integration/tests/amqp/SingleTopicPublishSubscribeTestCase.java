package org.wso2.mb.integration.tests.amqp;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

/**
 * subscribe to a topic and send 1000 messages and verify if messages are being received
 */
public class SingleTopicPublishSubscribeTestCase {


    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "topic"})
    public void performSingleTopicPublishSubscribeTestCase() {

        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:hasitha",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:hasitha", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=1000", "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        if (sendSuccess && receiveSuccess) {
            System.out.println("TEST PASSED");
        } else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals(sendSuccess && receiveSuccess, true);
    }
}
