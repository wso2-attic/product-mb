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

package org.wso2.carbon.mb.ui.test.topic;

import org.apache.commons.configuration.ConfigurationException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.mb.integration.common.utils.backend.ConfigurationEditor;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationUiBaseTest;
import org.wso2.mb.integration.common.utils.ui.pages.login.LoginPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.HomePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.TopicAddPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.TopicSubscribePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.TopicSubscriptionsPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.TopicsBrowsePage;

import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.io.IOException;

/**
 * This tests the creation of a topic from management console where a different node ID is set in the broker.xml file.
 * JIRA - https://wso2.org/jira/browse/MB-1084
 */
public class TopicCreationDifferentNodeIDTestCase extends MBIntegrationUiBaseTest {

    /**
     * Initializes the test case by modifying the "nodeId" value in broker.xml and restarting the server.
     *
     * @throws org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException
     * @throws javax.xml.xpath.XPathExpressionException
     * @throws java.net.MalformedURLException
     */
    @BeforeClass()
    public void initialize() throws AutomationUtilException, XPathExpressionException, IOException,
                                                                                                ConfigurationException {
        super.init();

        super.serverManager = new ServerConfigurationManager(mbServer);
        String defaultMBConfigurationPath = ServerConfigurationManager.getCarbonHome() + File.separator + "repository" +
                                            File.separator + "conf" + File.separator + "broker.xml";
        ConfigurationEditor configurationEditor = new ConfigurationEditor(defaultMBConfigurationPath);
        // Changing "nodeID" value to "home" in broker.xml
        configurationEditor.updateProperty(AndesConfiguration.COORDINATION_NODE_ID, "home");
        // Restarting server
        configurationEditor.applyUpdatedConfigurationAndRestartServer(serverManager);
    }

    /**
     * Tests adding of subscriptions to topic from UI
     * <p/>
     * Test Steps:
     * - Login to management console
     * - Create a topic
     * - Adds a subscription to topic
     * - Goes to topic subscription page
     * - Checks whether subscription is creates
     *
     * @throws XPathExpressionException
     * @throws java.io.IOException
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void topicCreationDifferentNodeIDTestCase() throws XPathExpressionException, IOException {

        String topicName = "diff-node-id-topic";
        driver.get(getLoginURL());

        // Logging in to the management console.
        LoginPage loginPage = new LoginPage(driver);
        HomePage homePage = loginPage.loginAs(getCurrentUserName(), getCurrentPassword());

        // Get current durable active subscriptions count
        TopicSubscriptionsPage topicSubscriptionsPage = homePage.getTopicSubscriptionsPage();
        int durableActiveSubscriptionsCountBeforeAdding = topicSubscriptionsPage.getDurableActiveSubscriptionsCount();

        // Adding a new topic.
        TopicAddPage topicAddPage = homePage.getTopicAddPage();
        Assert.assertEquals(topicAddPage.addTopic(topicName), true);

        // Check if topic is present
        TopicsBrowsePage topicBrowsePage = homePage.getTopicsBrowsePage();

        // Adding subscription
        TopicSubscribePage topicSubscribePage = topicBrowsePage.addSubscription(topicName);
        boolean subscriptionAdded = topicSubscribePage.addSubscription("Topic Only", "http://www.google.com", null);
        Assert.assertTrue(subscriptionAdded, "Subscription was not added successfully");

        // Get current durable active subscriptions count
        topicSubscriptionsPage = homePage.getTopicSubscriptionsPage();
        int durableActiveSubscriptionsCountAfterAdding = topicSubscriptionsPage.getDurableActiveSubscriptionsCount();

        // Check if subscription was added successfully.
        Assert.assertEquals(durableActiveSubscriptionsCountAfterAdding, durableActiveSubscriptionsCountBeforeAdding + 1,
                                                            "Subscription has not changed after adding a subscription");
    }

    /**
     * Shuts down the selenium web driver and resets to default configuration.
     *
     * @throws IOException
     * @throws AutomationUtilException
     */
    @AfterClass()
    public void tearDown() throws IOException, AutomationUtilException {
        //Revert back to original configuration.
        super.serverManager.restoreToLastConfiguration(true);
        driver.quit();
    }
}
