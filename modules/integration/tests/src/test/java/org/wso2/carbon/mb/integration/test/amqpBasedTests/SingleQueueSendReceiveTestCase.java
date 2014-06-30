package org.wso2.carbon.mb.integration.test.amqpBasedTests;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.mb.integration.test.MbMasterTestCase;
import org.wso2.carbon.mb.integration.test.amqpBasedTests.client.AndesClient;
import org.wso2.carbon.mb.integration.test.amqpBasedTests.utils.AndesClientUtils;

import java.rmi.RemoteException;

/**
 * Send messages to a single queue and consume them and see if sent messages are received
 */
public class SingleQueueSendReceiveTestCase extends MbMasterTestCase{


    @BeforeClass
    public void prepare() throws RemoteException, LoginAuthenticationExceptionException {
         init(2);
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue"})
    public void performSingleQueueSendReceiveTestCase() {
        //getEnvironment().getBackEndUrl();
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:singleQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:singleQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        if(receiveSuccess && sendSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals((receiveSuccess && sendSuccess), true);
    }


}
