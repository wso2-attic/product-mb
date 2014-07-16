package org.wso2.mb.integration.tests.jms.expiration;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

/**
 * Created with IntelliJ IDEA.
 * User: hasithad
 * Date: 7/11/14
 * Time: 1:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class MixedDurableTopicTestCase {

    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }


    @Test(groups={"wso2.mb", "durableTopic"})
    public void performDurableTopicTestCase() {

        //TODO hasithad - revisit after fixing issues in unsubscribing from durable topics

        Integer sendNormalCount = 1500;
        //Integer sendExpiredCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 500;
        String expiration = "100";


        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false",
                runTime.toString(), sendNormalCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendNormalCount, "");

        sendingClient.startWorking();

        /*AndesClient sendingClient2 = new AndesClient("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false",
                runTime.toString(), sendExpiredCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendExpiredCount, "",expiration);

        sendingClient2.startWorking();   */

        boolean receivingSuccess1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount , runTime);

        boolean sendingSuccess1 = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendNormalCount);
        //boolean sendingSuccess2 = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendExpiredCount);

        //we just closed the subscription. Rest of messages should be delivered now.

        AndesClientUtils.sleepForInterval(2000);

        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0,unsubscribeAfter="+expectedCount+",stopAfter="+expectedCount, "");
        receivingClient2.startWorking();

        boolean receivingSuccess2 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2, expectedCount , runTime);

        //now we have unsubscribed the topic subscriber no more messages should be received

        AndesClientUtils.sleepForInterval(2000);

        AndesClient receivingClient3 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=sub1,delayBetweenMsg=0,unsubscribeAfter="+expectedCount+",stopAfter="+expectedCount, "");
        receivingClient3.startWorking();

        boolean receivingSuccess3 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3, expectedCount , runTime);

        if(sendingSuccess1 && sendingSuccess1 && receivingSuccess1 && receivingSuccess2 && !receivingSuccess3) {
            System.out.println("TEST PASSED");
        } else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals((sendingSuccess1 && sendingSuccess1 && receivingSuccess1 && receivingSuccess2 && !receivingSuccess3), true);

        //Assert.assertEquals();
    }
}
