package org.wso2.mb.integration.tests.jms.expiration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import static org.testng.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: hasithad
 * Date: 7/11/14
 * Time: 1:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class MixedQueueTestCase extends MBIntegrationBaseTest {

    private static final Log log = LogFactory.getLog(MixedQueueTestCase.class);

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = "wso2.mb", description = "Single queue send-receive test case with 50% expired messages")
    public void performSingleQueueSendReceiveTestCase() {
        Integer sendNormalCount = 600;
        Integer sendExpiredCount = 400;
        String expiration = "100";
        Integer receiverRunTime = 30;
        Integer runTime = 20;
        Integer expectedCount = 600;
        Integer delayBetweenMsg= 0;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:singleQueue",
                "100", "false", receiverRunTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg="+delayBetweenMsg+",stopAfter="+expectedCount, "","");


        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
                runTime.toString(), sendNormalCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendNormalCount, "", "");

        sendingClient.startWorking();

        AndesClient sendingWithExpirationClient = new AndesClient("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
                runTime.toString(), sendExpiredCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendExpiredCount, "", expiration);

        sendingWithExpirationClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, receiverRunTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,expectedCount);

        if(receiveSuccess && sendSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }
        assertEquals((receiveSuccess && sendSuccess), true);


    }


    @Test(groups = "wso2.mb", description = "subscribe to a topic and send message to a queue which has the same name as queue")
    public void performSubTopicPubQueueTestCase() {

        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:topic1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:topic1", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        assertEquals((receiveSuccess && sendSuccess), false);
    }


    @Test(groups = "wso2.mb", description = "send large number of messages to a queue which has two consumers")
    public void performManyConsumersTestCase() {

        Integer sendNormalCount = 1000;
        Integer sendExpiredCount = 2000;
        Integer runTime = 20;
        Integer expectedCount = 1000;
        String  expiration = "100";

        AndesClient receivingClient1 = new AndesClient("receive", "127.0.0.1:5672", "queue:queue1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "","");

        receivingClient1.startWorking();

        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672", "queue:queue1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "","");
        receivingClient2.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:queue1", "100", "false",
                runTime.toString(), sendNormalCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendNormalCount, "","");

        sendingClient.startWorking();

        AndesClient sendingClient2 = new AndesClient("send", "127.0.0.1:5672", "queue:queue1", "100", "false",
                runTime.toString(), sendExpiredCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendExpiredCount, "",expiration);

        sendingClient2.startWorking();

        int msgCountFromClient1 = AndesClientUtils.getNoOfMessagesReceived(receivingClient1, expectedCount, runTime);
        int msgCountFromClient2 = AndesClientUtils.getNoOfMessagesReceived(receivingClient2, expectedCount, runTime);

        assertEquals(msgCountFromClient1+msgCountFromClient2,expectedCount.intValue());
    }



}
