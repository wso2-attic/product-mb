package org.wso2.mb.integration.tests.amqp.functional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.topic.BasicTopicSubscriber;

import javax.jms.JMSException;
import javax.naming.NamingException;

/**
 * This class holds set of test cases to verify if durable topic
 * subscriptions happen according to spec.
 */
public class DurableTopicSubscriptionTestCase {

    private static Log log = LogFactory.getLog(DurableTopicSubscriptionTestCase.class);
    private String host = "127.0.0.1";
    private String port = "5672";
    private String userName = "admin";
    private String password = "admin";
    private Integer runTime = 10;
    private Integer expectedCount = 10;
    private long intervalBetSubscription = 1000;

    /**
     * Creating a client with a subscription ID and unsubscribe it and
     * create another client with the same subscription ID
     *
     * @throws JMSException
     * @throws NamingException
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void basicSubscriptionTest() throws JMSException, NamingException {

        /**
         * create with sub id= x topic=y. disconnect and try to connect again
         */

        try {
            String topic = "myTopic1";
            String subID = "wso2";
            Integer runTime = 10;
            Integer expectedCount = 10;
            AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:" + topic,
                    "100", "false", runTime.toString(), expectedCount.toString(),
                    "1", "listener=true,ackMode=1,durable=true,subscriptionID=" + subID + ",delayBetweenMsg=0," +
                    "unsubscribeAfter=" + expectedCount, "");
            receivingClient.startWorking();
            sleepForInterval(intervalBetSubscription);
            receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:" + topic,
                    "100", "false", runTime.toString(), expectedCount.toString(),
                    "1", "listener=true,ackMode=1,durable=true,subscriptionID=" + subID + ",delayBetweenMsg=0," +
                    "unsubscribeAfter=" + expectedCount, "");
            receivingClient.startWorking();
            sleepForInterval(intervalBetSubscription);
        } finally {

        }


        /**
         * create with sub id= x topic=y. kill subscription and try to connect again
         */
        //Cannot automate

    }


    /**
     * create with sub id= x topic=y. try another subscription with same params.
     * should rejects the subscription
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void multipleSubsWithSameIdTest() throws JMSException, NamingException {

        String topic = "myTopic2";
        String subID = "sriLanka";
        BasicTopicSubscriber sub1 = null;
        BasicTopicSubscriber sub2 = null;
        boolean multipleSubsNotAllowed = true;
        try {
            sub1 = new BasicTopicSubscriber(host, port, userName, password, topic);
            sub1.subscribe(topic, true, subID);
            sleepForInterval(intervalBetSubscription);
            try {
                sub2 = new BasicTopicSubscriber(host, port, userName, password, topic);
                sub2.subscribe(topic, true, subID);
                sleepForInterval(intervalBetSubscription);
            } catch (Exception e) {
                log.error("Error while subscribing. This is expected.", e);
                multipleSubsNotAllowed = false;

            }
            Assert.assertFalse(multipleSubsNotAllowed, "Multiple subscriptions allowed for same client ID.");
        } finally {
            if (null != sub1) {
                sub1.close();
            }
        }
        Assert.assertFalse(multipleSubsNotAllowed, "Multiple subscriptions allowed for same client ID.");

    }

    /**
     * create with sub id= x topic=y. try another with sub id=z topic=y. Allowed
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void multipleSubsWithDifferentIdTest() throws JMSException, NamingException {

        String topic = "myTopic3";
        String subID1 = "test1";
        String subID2 = "test2";
        BasicTopicSubscriber sub1 = new BasicTopicSubscriber(host, port, userName, password, topic);
        sub1.subscribe(topic, true, subID1);
        sleepForInterval(intervalBetSubscription);
        BasicTopicSubscriber sub2 = new BasicTopicSubscriber(host, port, userName, password, topic);
        sub2.subscribe(topic, true, subID2);
        sleepForInterval(intervalBetSubscription);

        /**
         * above multiple subscribers closed
         */
        sub1.close();
        sub2.close();
    }

    /**
     * create with sub id= x topic=y.
     * close it.
     * Then try with sub id= x topic=z.
     * Should reject the subscription
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void multipleSubsToDifferentTopicsWithSameSubIdTest() throws JMSException, NamingException {

        String topic1 = "myTopic4";
        String topic2 = "myTopic5";
        String subID1 = "test3";
        boolean subscriptionAllowedForDifferentTopic = true;
        BasicTopicSubscriber sub1 = new BasicTopicSubscriber(host, port, userName, password, topic1);
        sub1.subscribe(topic1, true, subID1);
        sleepForInterval(intervalBetSubscription);
        sub1.close();
        sleepForInterval(intervalBetSubscription);
        try {
            BasicTopicSubscriber sub2 = new BasicTopicSubscriber(host, port, userName, password, topic2);
            sub2.subscribe(topic2, true, subID1);
            sleepForInterval(intervalBetSubscription);
        } catch (JMSException e) {
            if (e.getMessage().contains("An Exclusive Bindings already exists for different topic. Not permitted")) {
                log.error("Error while subscribing. This is expected.", e);
                subscriptionAllowedForDifferentTopic = false;
            } else {
                log.error("Error while subscribing.", e);
                throw new JMSException("Error while subscribing");
            }
        }
        Assert.assertFalse(subscriptionAllowedForDifferentTopic, "Subscriptions to a different topic" +
                " was allowed by same client Id without un-subscribing");
    }

    /**
     * create with sub id= x topic=y.
     * Create a normal topic subscription topic=y
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void durableTopicWithNormalTopicTest() throws JMSException, NamingException {

        String topic = "myTopic5";
        String subID = "test5";

        BasicTopicSubscriber sub1 = null;
        BasicTopicSubscriber sub2 = null;
        try {
            sub1 = new BasicTopicSubscriber(host, port, userName, password, topic);
            sub1.subscribe(topic, true, subID);
            sleepForInterval(intervalBetSubscription);

            sub2 = new BasicTopicSubscriber(host, port, userName, password, topic);
            sub2.subscribe(topic, false, "");
            sleepForInterval(intervalBetSubscription);
        } finally {
            if (null != sub1) {
                sub1.close();
            }
            if (null != sub2) {
                sub2.close();
            }
        }
    }

    /**
     * create with sub id= x topic=y.
     * Unsubscribe.
     * Now try sub id= x topic=y
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void subscribeUnSuscribeAndSubscribeAgainTest() throws JMSException, NamingException {

        String topic = "myTopic7";
        String subID = "test7";


        try {
            AndesClient sub1 = new AndesClient("receive", "127.0.0.1:5672", "topic:" + topic,
                    "100", "false", runTime.toString(), expectedCount.toString(),
                    "1", "listener=true,ackMode=1,durable=true,subscriptionID=" + subID + ",delayBetweenMsg=0," +
                    "unsubscribeAfter=" + expectedCount, "");


            sub1.startWorking();
            sleepForInterval(intervalBetSubscription);

            AndesClient sub2 = new AndesClient("receive", "127.0.0.1:5672", "topic:" + topic,
                    "100", "false", runTime.toString(), expectedCount.toString(),
                    "1", "listener=true,ackMode=1,durable=true,subscriptionID=" + subID + ",delayBetweenMsg=0," +
                    "unsubscribeAfter=" + expectedCount, "");

            sub2.startWorking();
            sleepForInterval(intervalBetSubscription);

        } finally {


        }

    }

    /**
     * create with sub id= x topic=y.
     * Unsubscribe.
     * Now try sub id= z topic=y
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void multipleSubsWithDiffIDsToSameTopicTest() throws JMSException, NamingException {
        String topic = "multiSubTopic";
        String subID1 = "new1";
        String subID2 = "new2";
        String subID3 = "new3";
        String subID4 = "new4";

        BasicTopicSubscriber sub1 = null;
        BasicTopicSubscriber sub2 = null;
        BasicTopicSubscriber sub3 = null;
        BasicTopicSubscriber sub4 = null;

        try {
            sub1 = new BasicTopicSubscriber(host, port, userName, password, topic);
            sub1.subscribe(topic, true, subID1);
            sleepForInterval(intervalBetSubscription);

            sub2 = new BasicTopicSubscriber(host, port, userName, password, topic);
            sub2.subscribe(topic, true, subID2);
            sleepForInterval(intervalBetSubscription);

            sub3 = new BasicTopicSubscriber(host, port, userName, password, topic);
            sub3.subscribe(topic, true, subID3);
            sleepForInterval(intervalBetSubscription);

            sub4 = new BasicTopicSubscriber(host, port, userName, password, topic);
            sub4.subscribe(topic, true, subID4);
            sleepForInterval(intervalBetSubscription);

        } finally {
            if (null != sub1) {
                sub1.close();
            }
            if (null != sub2) {
                sub2.close();
            }
            if (null != sub3) {
                sub3.close();
            }
            if (null != sub4) {
                sub4.close();
            }
        }
    }

    /**
     * create with sub id= x topic=y.
     * Unsubscribe.
     * Now try sub id= x topic=z
     */

    @Test(groups = {"wso2.mb", "topic"})
    public void subscribeUnsubscribeAndTryDifferentTopicTest()
            throws JMSException, NamingException {

        String topic1 = "myTopic8";
        String topic2 = "myTopic9";
        String subID = "test8";


        AndesClient sub1 = new AndesClient("receive", "127.0.0.1:5672", "topic:" + topic1,
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=" + subID + ",delayBetweenMsg=0," +
                "unsubscribeAfter=" + expectedCount, "");
        sub1.startWorking();
        sleepForInterval(intervalBetSubscription);

        sub1 = new AndesClient("receive", "127.0.0.1:5672", "topic:" + topic1,
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=" + subID + ",delayBetweenMsg=0," +
                "unsubscribeAfter=" + expectedCount, "");
        sub1.startWorking();
    }

    private void sleepForInterval(long timeToSleep) {
        try {
            Thread.sleep(timeToSleep);
        } catch (InterruptedException e) {
            //ignore
        }
    }
}
