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

package org.wso2.carbon.mb.ui.test.jira;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.automation.engine.frameworkutils.FrameworkPathUtil;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.ConfigurationEditor;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationUiBaseTest;
import org.wso2.mb.integration.common.utils.ui.pages.login.LoginPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.HomePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueueAddPage;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Refer wso2 jira : https://wso2.org/jira/browse/MB-939 for details.
 * Verify that the maximum display length is configurable for message content shown through management console.
 */
public class MB939TestCase extends MBIntegrationUiBaseTest {

    private static final Log log = LogFactory.getLog(MB939TestCase.class);

    private static final int messageSizeInBytes = 1024 * 1024; //Size of MessageContentInput.txt
    private static final String TEST_QUEUE_NAME = "939TestQueue";
    // Input file to read a 1MB message content.
    private static final String messageContentInputFilePath = System.getProperty("framework.resource.location") + File.separator +
            "MessageContentInput.txt";

    private static final String defaultMBConfigurationPath = ServerConfigurationManager.getCarbonHome() +
            File.separator + "repository" + File.separator + "conf" + File.separator + "broker.xml";

    @BeforeClass()
    public void init() throws Exception {
        super.init();
    }

    /**
     * Increase the managementConsole/maximumMessageDisplayLength to match the large message size that is tested.
     */
    @BeforeClass
    public void setupConfiguration() throws Exception {

        super.serverManager = new ServerConfigurationManager(mbServer);

        ConfigurationEditor configurationEditor = new ConfigurationEditor(defaultMBConfigurationPath);

        configurationEditor.updateProperty(AndesConfiguration.MANAGEMENT_CONSOLE_MAX_DISPLAY_LENGTH_FOR_MESSAGE_CONTENT,String.valueOf(messageSizeInBytes));

        configurationEditor.applyUpdatedConfigurationAndRestartServer(serverManager);
    }

    /**
     * Create a Queue and successfully send a large message to the Queue
     */
    @BeforeClass()
    public void publishLargeMessageToQueue() throws Exception {

        // Login and create test Queue
        driver.get(getLoginURL());
        LoginPage loginPage = new LoginPage(driver);
        HomePage homePage = loginPage.loginAs(mbServer.getContextTenant()
                .getContextUser().getUserName(), mbServer.getContextTenant()
                .getContextUser().getPassword());

        QueueAddPage queueAddPage = homePage.getQueueAddPage();
        Assert.assertEquals(queueAddPage.addQueue(TEST_QUEUE_NAME), true);

    }

    /**
     * Verify that the Message content browse page for the sent message displays the exact length as the original message.
     */
    @Test()
    public void verifyDisplayedMessageContentLength() {

    }

    /**
     * Revert changed configuration, purge and delete the queue.
     */
    @AfterClass()
    public void cleanup() {

    }



}
