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
import org.wso2.mb.integration.common.clients.operations.topic.BasicTopicSubscriber;
import org.wso2.mb.integration.common.clients.operations.topic.TopicAdminClient;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;

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

    }

    /**
     * Create with sub id= x topic=y. Disconnect and try to connect again from a different node.
     */
    @Test(groups = "wso2.mb", description = "Reconnect to topic with same sub ID after " +
            "disconnecting", enabled = true)
    public void basicSubscriptionTest() throws JMSException, NamingException {

        BasicTopicSubscriber sub1 = null;
        try {
            String topic = "durableTopic1";
            String subID = "wso2sub1";
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
                sub1.unsubscribe("wso2sub1");
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
        String subID = "wso2sub1";
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
                sub1.unsubscribe("wso2sub1");
            }
        }
    }

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
        topicAdminClient1.removeTopic("durableTopic1");
        topicAdminClient1.removeTopic("durableTopic2");

    }

}
