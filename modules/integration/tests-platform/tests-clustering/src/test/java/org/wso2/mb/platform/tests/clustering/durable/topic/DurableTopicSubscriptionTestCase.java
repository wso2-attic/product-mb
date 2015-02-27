/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.platform.tests.clustering.durable.topic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.clients.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.topic.BasicTopicSubscriber;
import org.wso2.mb.integration.common.clients.operations.clients.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

/**
 *
 */
public class DurableTopicSubscriptionTestCase extends MBPlatformBaseTest {

    private static Log log = LogFactory.getLog(DurableTopicSubscriptionTestCase.class);
    private String hostNode1;
    private String hostNode2;
    private String portInNode1;
    private String portInNode2;
    private String userName = "admin";
    private String password = "admin";
    private long intervalBetSubscription = 100;

    private TopicAdminClient topicAdminClient1;

    /**
     * Prepare environment for tests.
     *
     * @throws Exception
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);

        AutomationContext automationContext1 = getAutomationContextWithKey("mb002");
        AutomationContext automationContext2 = getAutomationContextWithKey("mb003");
        hostNode1 = automationContext1.getInstance().getHosts().get("default");
        hostNode2 = automationContext2.getInstance().getHosts().get("default");
        portInNode1 = automationContext1.getInstance().getPorts().get("amqp");
        portInNode2 = automationContext2.getInstance().getPorts().get("amqp");
        topicAdminClient1 = new TopicAdminClient(automationContext1.getContextUrls().getBackEndUrl(),
                super.login(automationContext1), ConfigurationContextProvider.getInstance().getConfigurationContext());

        super.initAndesAdminClients();

    }

    /**
     * Create with sub id= x topic=y. Disconnect and try to connect again from a different node.
     */
    @Test(groups = "wso2.mb", description = "Reconnect to topic with same sub ID after " +
            "disconnecting", enabled = true)
    public void subscribeDisconnectAndSubscribeAgainTest() throws JMSException, NamingException {

        BasicTopicSubscriber sub1 = null;
        String topic = "durableTopic1";
        String subID = "durableTopic1Sub";
        try {
            sub1 = new BasicTopicSubscriber(hostNode1, portInNode1, userName, password, topic);
            sub1.subscribe(topic, true, subID);
            sleepForInterval(intervalBetSubscription);
            sub1.close();
            sleepForInterval(intervalBetSubscription);
            sub1 = new BasicTopicSubscriber(hostNode2, portInNode2, userName, password, topic);
            sub1.subscribe(topic, true, subID);
            sleepForInterval(intervalBetSubscription);
        } finally {
            if (null != sub1) {
                sub1.unsubscribe(subID);
            }
        }

    }

    /**
     * Create with sub id= x topic=y. try another subscription from a different node with same
     * params.Should rejects the subscription
     */
    @Test(groups = "wso2.mb", description = "Try to connect to a topic with same subscription ID " +
            "which is already a subscription", enabled = true)
    public void multipleSubsWithSameIdTest() throws JMSException, NamingException {

        String topic = "durableTopic2";
        String subID = "durableTopic2Sub";
        BasicTopicSubscriber sub1 = null;
        BasicTopicSubscriber sub2;
        boolean multipleSubsNotAllowed = true;
        try {
            sub1 = new BasicTopicSubscriber(hostNode1, portInNode1, userName, password, topic);
            sub1.subscribe(topic, true, subID);
            sleepForInterval(intervalBetSubscription);
            try {
                sub2 = new BasicTopicSubscriber(hostNode2, portInNode2, userName, password,
                        topic);
                sub2.subscribe(topic, true, subID);

                sleepForInterval(intervalBetSubscription);
            } catch (JMSException e) {
                if (e.getMessage().contains("as it already has an existing exclusive consumer")) {
                    log.error("Error while subscribing. This is expected.", e);
                    multipleSubsNotAllowed = false;
                } else {
                    log.error("Error while subscribing", e);
                    throw new JMSException("Error while subscribing");
                }
            }
            Assert.assertFalse(multipleSubsNotAllowed, "Multiple subscriptions allowed for same client ID.");
        } finally {
            if (null != sub1) {
                sub1.unsubscribe(subID);
            }
        }
    }

    /**
     * Create with sub id= x topic=y. try another subscription from a different node with a
     * different subscription ID.Should allow the subscription
     */
    @Test(groups = "wso2.mb", description = "Try to connect to same topic with different " +
            "subscription IDs", enabled = true)
    public void multipleSubToSameTopicTest() throws JMSException, NamingException {

        String topic = "durableTopic3";
        String subID1 = "durableTopic3Sub1";
        String subID2 = "durableTopic3Sub2";
        BasicTopicSubscriber sub1 = null;
        BasicTopicSubscriber sub2 = null;
        try {
            sub1 = new BasicTopicSubscriber(hostNode1, portInNode1, userName, password, topic);
            sub1.subscribe(topic, true, subID1);
            sleepForInterval(intervalBetSubscription);

            sub2 = new BasicTopicSubscriber(hostNode2, portInNode2, userName, password,
                    topic);
            sub2.subscribe(topic, true, subID2);

            sleepForInterval(intervalBetSubscription);

        } finally {
            if (null != sub1) {
                sub1.unsubscribe(subID1);
            }
            if (null != sub2) {
                sub2.unsubscribe(subID2);
            }
        }
    }

    /**
     * Create with sub id= x topic=y. Unsubscribe and try to connect again from a different node.
     */
    @Test(groups = "wso2.mb", description = "Reconnect to topic with same sub ID after " +
            "unsubscribing", enabled = true)
    public void subscribeUnsubscribeAndSubscribeAgainTest() throws JMSException, NamingException {

        String topic = "durableTopic4";
        String subID = "durableTopic4Sub1";
        BasicTopicSubscriber sub1 = null;
        try {
            sub1 = new BasicTopicSubscriber(hostNode1, portInNode1, userName, password, topic);
            sub1.subscribe(topic, true, subID);
            sleepForInterval(intervalBetSubscription);
            sub1.unsubscribe(subID);
            sleepForInterval(intervalBetSubscription);
            sub1 = new BasicTopicSubscriber(hostNode2, portInNode2, userName, password,
                    topic);
            sub1.subscribe(topic, true, subID);

            sleepForInterval(intervalBetSubscription);

        } finally {
            if (null != sub1) {
                sub1.unsubscribe(subID);
            }
        }
    }

    /**
     * Create with sub id= x topic=y. Unsubscribe and try to connect another subscription for the
     * same topic from a different node.
     */
    @Test(groups = "wso2.mb", description = "Reconnect to topic with different sub ID after " +
            "unsubscribing", enabled = true)
    public void subscribeUnsubscribeWithDifferentIDsTest() throws JMSException, NamingException {

        String topic = "durableTopic5";
        String subID1 = "durableTopic5Sub1";
        String subID2 = "durableTopic5Sub2";
        BasicTopicSubscriber sub1;
        BasicTopicSubscriber sub2 = null;
        try {
            sub1 = new BasicTopicSubscriber(hostNode1, portInNode1, userName, password, topic);
            sub1.subscribe(topic, true, subID1);
            sleepForInterval(intervalBetSubscription);
            sub1.unsubscribe(subID1);
            sleepForInterval(intervalBetSubscription);
            sub2 = new BasicTopicSubscriber(hostNode2, portInNode2, userName, password,
                    topic);
            sub2.subscribe(topic, true, subID2);

            sleepForInterval(intervalBetSubscription);

        } finally {
            if (null != sub2) {
                sub2.unsubscribe(subID2);
            }
        }
    }

    /**
     * Create with sub id= x topic=y. Unsubscribe. Then try to connect with the same subscription
     * to a different topic from another node
     */
    @Test(groups = "wso2.mb", description = "Connect to a different topic with same sub ID after " +
            "unsubscribing", enabled = true)
    public void sameIdDifferentTopicsTest() throws JMSException, NamingException {

        String topic1 = "durableTopic6";
        String topic2 = "durableTopic7";
        String subID = "durableTopic6Sub1";
        BasicTopicSubscriber sub1 = null;
        try {
            sub1 = new BasicTopicSubscriber(hostNode1, portInNode1, userName, password, topic1);
            sub1.subscribe(topic1, true, subID);
            sleepForInterval(intervalBetSubscription);
            sub1.unsubscribe(subID);
            sleepForInterval(intervalBetSubscription);
            sub1 = new BasicTopicSubscriber(hostNode2, portInNode2, userName, password,
                    topic2);
            sub1.subscribe(topic2, true, subID);

            sleepForInterval(intervalBetSubscription);

        } finally {
            if (null != sub1) {
                sub1.unsubscribe(subID);
            }
        }
    }

    /**
     * Create with sub id= x topic=y
     * Create with sub id= z topic=y
     * Create a normal topic subscriber topic=y
     * Create a normal queue subscriber queue=y
     *
     * @throws JMSException
     * @throws NamingException
     */
    @Test(groups = "wso2.mb", description = "Create all kinds of subscriptions for same " +
            "topic/queue name", enabled = true)
    public void allKindOfSubscriptionsTest()
            throws JMSException, NamingException, XPathExpressionException,
                   AndesClientConfigurationException {

        String topicName = "wso2";
        String queueName = "wso2";
        String subID1 = "wso2Sub1";
        String subID2 = "wso2Sub2";
        BasicTopicSubscriber durableTopicsub1 = null;
        BasicTopicSubscriber durableTopicsub2 = null;
        BasicTopicSubscriber nonDurableTopicsub = null;
        AndesClient receivingClient = null;
        Integer expectedCount = 1000;
        try {
            durableTopicsub1 = new BasicTopicSubscriber(hostNode1, portInNode1, userName,
                    password, topicName);
            durableTopicsub1.subscribe(topicName, true, subID1);
            sleepForInterval(intervalBetSubscription);

            durableTopicsub2 = new BasicTopicSubscriber(hostNode2, portInNode2, userName, password,
                    topicName);
            durableTopicsub2.subscribe(topicName, true, subID2);
            sleepForInterval(intervalBetSubscription);

            nonDurableTopicsub = new BasicTopicSubscriber(hostNode1, portInNode1, userName,
                    password, topicName);
            nonDurableTopicsub.subscribe(topicName, false, null);

            //subscribe for a queue with a same queue name as the topic
            String randomInstanceKey = getRandomMBInstance();
            AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

            // Creating a initial JMS consumer client configuration
            AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(tempContext.getInstance().getHosts().get("default"),
                                                                                                         Integer.parseInt(tempContext.getInstance().getPorts().get("amqp")),
                                                                                                         ExchangeType.QUEUE, queueName);
            // Amount of message to receive
            consumerConfig.setMaximumMessagesToReceived(expectedCount);
            consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

            receivingClient = new AndesClient(consumerConfig, true);
            receivingClient.startClient();


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != durableTopicsub1) {
                durableTopicsub1.unsubscribe(subID1);
            }
            if (null != durableTopicsub2) {
                durableTopicsub2.unsubscribe(subID2);
            }
            if (null != nonDurableTopicsub) {
                nonDurableTopicsub.close();
            }
            if (null != receivingClient) {
                receivingClient.stopClient();
            }

        }
    }

    /**
     * Sleep the current thread
     * @param timeToSleep sleep time in seconds
     */
    private void sleepForInterval(long timeToSleep) {
        try {
            Thread.sleep(timeToSleep);
        } catch (InterruptedException ignored) {
            //ignore
        }
    }

    /**
     * Cleanup after running tests.
     *
     * @throws Exception
     */
    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        //deleting the topics created
        topicAdminClient1.removeTopic("durableTopic1");
        topicAdminClient1.removeTopic("durableTopic2");
        topicAdminClient1.removeTopic("durableTopic3");
        topicAdminClient1.removeTopic("durableTopic4");
        topicAdminClient1.removeTopic("durableTopic5");
        topicAdminClient1.removeTopic("durableTopic6");
        topicAdminClient1.removeTopic("durableTopic7");
        topicAdminClient1.removeTopic("wso2");

        //deleting the queue created
        String randomInstanceKey = getRandomMBInstance();
        AndesAdminClient tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);
        if (null != tempAndesAdminClient.getQueueByName("wso2")) {
            tempAndesAdminClient.deleteQueue("wso2");
        }

    }

}
