package org.wso2.mb.integration.tests.amqp;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;


/**
 * Test with #,* one level two levels
 */
public class HierarchicalTopicsTestCase {

    static Integer sendCount = 1000;
    static Integer runTime = 20;
    static Integer expectedCount = 1000;


    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "topic"})
    public void performHierarchicalTopicsTopicOnlyTestCase() {

        /**
         * topic only option. Here we subscribe to games.cricket and verify that only messages
         * specifically published to games.cricket is received
         */
        System.out.println("*********topic only test*******************");
        boolean topicOnlySuccess = false;

        //we should not get any message here
        AndesClient receivingClient1 = getReceivingClientforTopic("games.cricket");
        receivingClient1.startWorking();

        AndesClient sendingClient1 = getSendingClientForTopic("games");
        sendingClient1.startWorking();

        boolean receiveSuccess1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1,expectedCount,runTime);

        //now we send messages specific to games.cricket topic. We should receive messages here
        AndesClientUtils.sleepForInterval(1000);

        AndesClient receivingClient2 = getReceivingClientforTopic("games.cricket");
        receivingClient2.startWorking();

        AndesClient sendingClient2 = getSendingClientForTopic("games.cricket");
        sendingClient2.startWorking();

        boolean receiveSuccess2 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2,expectedCount,runTime);

        topicOnlySuccess = (!receiveSuccess1 && receiveSuccess2);

        System.out.println("topicOnlySuccess: " + topicOnlySuccess + " receiveSuccess1:" +
                receiveSuccess1 + " receiveSuccess2:" + receiveSuccess2);

        if(topicOnlySuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals(topicOnlySuccess, true);

        AndesClientUtils.sleepForInterval(1000);
    }


    /**
     * immediate children option. Here you subscribe to the first level of sub-topics but not to the topic itself.
     * 1. subscribe to games.* and publish to games. Should receive no message
     * 2. subscribe to games.* and publish to games.football. Messages should be received
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void performHierarchicalTopicsImmediateChildrenTestCase() {

        System.out.println("*********immediate children test*******************");

        boolean immediateChildrenSuccess = false;

        //we should not get any message here
        AndesClient receivingClient3 = getReceivingClientforTopic("games.*");
        receivingClient3.startWorking();

        AndesClient sendingClient3 = getSendingClientForTopic("games");
        sendingClient3.startWorking();

        boolean receiveSuccess3 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient3,expectedCount,runTime);

        //now we send messages child to games.football. We should receive messages here
        AndesClientUtils.sleepForInterval(1000);

        AndesClient receivingClient4 = getReceivingClientforTopic("games.*");
        receivingClient4.startWorking();

        AndesClient sendingClient4 = getSendingClientForTopic("games.football");
        sendingClient4.startWorking();

        boolean receiveSuccess4 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient4,expectedCount,runTime);

        //now we send messages to a child that is not immediate. We should not receive messages
        AndesClientUtils.sleepForInterval(1000);

        AndesClient receivingClient5 = getReceivingClientforTopic("games.*");
        receivingClient5.startWorking();

        AndesClient sendingClient5 = getSendingClientForTopic("games.cricket.sriLanka");
        sendingClient5.startWorking();

        boolean receiveSuccess5 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient5,expectedCount,runTime);

        immediateChildrenSuccess = (!receiveSuccess3 && receiveSuccess4 && !receiveSuccess5);

        System.out.println("immediateChildrenSuccess: " + immediateChildrenSuccess + " receiveSuccess3:" +
                receiveSuccess3 + " receiveSuccess4:" + receiveSuccess4 + "receiveSuccess5:" + receiveSuccess5);

        if(immediateChildrenSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals(immediateChildrenSuccess, true);

        AndesClientUtils.sleepForInterval(1000);


    }

    /**
     *  topic and children option. Here messages published to topic itself and any level
     *  in the hierarchy should be received
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void performHierarchicalTopicsChildrenTestCase() {

        System.out.println("*********topic and children test*******************");

        boolean topicAndChildrenSuccess = false;

        //we should  get any message here
        AndesClient receivingClient6 = getReceivingClientforTopic("games.#");
        receivingClient6.startWorking();

        AndesClient sendingClient6 = getSendingClientForTopic("games");
        sendingClient6.startWorking();

        boolean receiveSuccess6 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient6,expectedCount,runTime);

        //now we send messages to level 2 child. We should receive messages here
        AndesClientUtils.sleepForInterval(1000);

        AndesClient receivingClient7 = getReceivingClientforTopic("games.#");
        receivingClient7.startWorking();

        AndesClient sendingClient7 = getSendingClientForTopic("games.football.sriLanka");
        sendingClient7.startWorking();

        boolean receiveSuccess7 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient7,expectedCount,runTime);

        topicAndChildrenSuccess = (receiveSuccess6 && receiveSuccess7);

        if(topicAndChildrenSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals(topicAndChildrenSuccess, true);
    }

    private  AndesClient getReceivingClientforTopic(String topicName) {
        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:"+topicName,
                "100", "false", runTime.toString() , expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");
        return receivingClient;
    }

    private  AndesClient getSendingClientForTopic(String topicName) {
        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:"+topicName, "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=1000", "");
        return sendingClient;
    }
}