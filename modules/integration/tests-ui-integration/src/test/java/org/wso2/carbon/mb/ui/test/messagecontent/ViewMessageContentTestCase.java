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

package org.wso2.carbon.mb.ui.test.messagecontent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.ConfigurationEditor;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationUiBaseTest;
import org.wso2.mb.integration.common.utils.ui.pages.login.LoginPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.HomePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.MessageContentPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueueAddPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueueContentPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueuesBrowsePage;

import java.io.File;

/**
 * Refer wso2 jira : https://wso2.org/jira/browse/MB-939 for details.
 * Verify that the maximum display length is configurable for message content shown through management console.
 */
public class ViewMessageContentTestCase extends MBIntegrationUiBaseTest {

    private static final Log log = LogFactory.getLog(ViewMessageContentTestCase.class);

    private static final int MESSAGE_SIZE_IN_BYTES = 1044375; //Size of MessageContentInput.txt
    private static final String TEST_QUEUE_NAME = "939TestQueue";
    // Input file to read a 1MB message content.
    private static final String MESSAGE_CONTENT_INPUT_FILE_PATH = System.getProperty("framework.resource.location") + File.separator +
            "MessageContentInput.txt";

    //private static final String DEFAULT_MB_CONFIG_PATH =

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

        String defaultMBConfigurationPath = ServerConfigurationManager.getCarbonHome() +
                File.separator + "repository" + File.separator + "conf" + File.separator + "broker.xml";

        log.info("DEFAULT_MB_CONFIG_PATH : " + defaultMBConfigurationPath);

        log.info("MESSAGE_CONTENT_INPUT_FILE_PATH" + MESSAGE_CONTENT_INPUT_FILE_PATH);

        ConfigurationEditor configurationEditor = new ConfigurationEditor(defaultMBConfigurationPath);

        configurationEditor.updateProperty(AndesConfiguration
                .MANAGEMENT_CONSOLE_MAX_DISPLAY_LENGTH_FOR_MESSAGE_CONTENT, String.valueOf(MESSAGE_SIZE_IN_BYTES + 1));

        configurationEditor.applyUpdatedConfigurationAndRestartServer(serverManager);
    }

    /**
     * Verify that the Message content browse page for the sent message displays the exact length as the original message.
     */
    @Test(groups = {"wso2.mb"})
    public void verifyDisplayedMessageContentLength() throws Exception {

        boolean testSuccess = false;
        int displayedLength;

        // Login and create test Queue
        driver.get(getLoginURL());
        LoginPage loginPage = new LoginPage(driver);
        HomePage homePage = loginPage.loginAs(mbServer.getContextTenant()
                .getContextUser().getUserName(), mbServer.getContextTenant()
                .getContextUser().getPassword());

        QueueAddPage queueAddPage = homePage.getQueueAddPage();
        Assert.assertEquals(queueAddPage.addQueue(TEST_QUEUE_NAME), true);

        Integer sendCount = 1;
        Integer runTime = 100;

        String queueNameArg = "queue:" + TEST_QUEUE_NAME;

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", queueNameArg, "100", "true",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,file=" + MESSAGE_CONTENT_INPUT_FILE_PATH + ",stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        QueuesBrowsePage queuesBrowsePage = homePage.getQueuesBrowsePage();

        QueueContentPage queueContentPage = queuesBrowsePage.browseQueue(TEST_QUEUE_NAME);
        Assert.assertNotNull(queueContentPage, "Unable to browse Queue " + TEST_QUEUE_NAME);

        MessageContentPage messageContentPage = queueContentPage.viewFullMessage(1);

        Assert.assertNotNull(messageContentPage, "Unable to view the fully sent large message to queue : " + TEST_QUEUE_NAME);

        displayedLength = messageContentPage.getDisplayedMessageLength();

        if (displayedLength == MESSAGE_SIZE_IN_BYTES) {
            testSuccess = true;
        }

        Assert.assertTrue(testSuccess, "Sent Large message of " + MESSAGE_SIZE_IN_BYTES + " bytes for queue " + TEST_QUEUE_NAME + " was not displayed correctly. " + "Displayed length : " + displayedLength);
    }

    /**
     * Revert changed configuration, purge and delete the queue.
     */
    @AfterClass()
    public void cleanup() throws Exception {

        // Delete test queue
        driver.get(getLoginURL());
        HomePage homePage = new HomePage(driver);

        QueuesBrowsePage queuesBrowsePage = homePage.getQueuesBrowsePage();

        Assert.assertTrue(queuesBrowsePage.deleteQueue(TEST_QUEUE_NAME), "Failed to delete queue : " + TEST_QUEUE_NAME);

        //Revert back to original configuration.
        super.serverManager.restoreToLastConfiguration(true);
    }



}
