/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.mb.ui.test.dlc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationUiBaseTest;
import org.wso2.mb.integration.common.utils.ui.UIElementMapper;
import org.wso2.mb.integration.common.utils.ui.pages.login.LoginPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.DLCBrowsePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.DLCContentPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.HomePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueueAddPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueueContentPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueuesBrowsePage;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.List;

/**
 * Test creating DeadLetter Channel and restoring,deleting and rerouting messages of DeadLetter Channel
 */
public class DLCTestCase extends MBIntegrationUiBaseTest {
    private static final Log log = LogFactory.getLog(DLCTestCase.class);
    private static final int COLUMN_LIST_SIZE = 11;
    private static final int MESSAGE_ID_COLUMN = 1;
    private static final long SEND_COUNT = 15L;
    private static final long EXPECTED_COUNT = 15L;

    @BeforeClass()
    public void init() throws Exception {
        super.init();
    }

    /**
     * Create a DeadLetter channel and send messages to DeadLetter Queue
     * which are failed to send
     */
    @BeforeClass()
    public void createDLC() throws AndesClientException, NamingException, JMSException,
                                   IOException {
        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "DLCTestQueue");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT + 200L);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE);
        consumerConfig.setAcknowledgeAfterEachMessageCount(215L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "DLCTestQueue");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        //Thread sleep until messages sent to DLC after breaching maximum number of retrying
        AndesClientUtils.sleepForInterval(100000L);
    }

    /**
     * Test restoring,deleting and rerouting messages of DeadLetter Channel
     *
     * @throws Exception
     */
    @Test()
    public void DLCTest() throws Exception {
        String qName = "DLCTestQueue";
        String rerouteQueue = "rerouteTestQueue";
        String deletingMessageID;
        String restoredMessageID;
        String restoringMessageID;
        String reroutingMessageID;
        String reroutedMessageID;

        driver.get(getLoginURL());
        LoginPage loginPage = new LoginPage(driver);
        HomePage homePage = loginPage.loginAs(mbServer.getContextTenant()
                                                      .getContextUser().getUserName(), mbServer
                                                      .getContextTenant().getContextUser().getPassword());
        //Add an queue to test rerouting messages of DLC
        QueueAddPage queueAddPage = homePage.getQueueAddPage();
        Assert.assertEquals(queueAddPage.addQueue(rerouteQueue), true);
        DLCBrowsePage dlcBrowsePage = homePage.getDLCBrowsePage();
        Assert.assertNotNull(dlcBrowsePage.isDLCCreated(),
                             "DeadLetter Channel not created. " + qName);
        //Testing delete messages
        DLCContentPage dlcContentPage = dlcBrowsePage.getDLCContent();
        deletingMessageID = dlcContentPage.deleteFunction();
        if (checkMessages(deletingMessageID, qName)) {
            log.info("Deleting messages of dead letter channel is successful.");
        } else {
            log.info("Deleting messages of dead letter channel is unsuccessful.");
        }

        //Testing restore messages
        restoringMessageID = dlcContentPage.restoreFunction();
        QueuesBrowsePage queuesBrowsePage = homePage.getQueuesBrowsePage();
        QueueContentPage queueContentPage = queuesBrowsePage.browseQueue(qName);
        if (isElementPresent(UIElementMapper.getInstance()
                                     .getElement("mb.dlc.browse.content.table"))) {
            restoredMessageID = driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                                    .getElement("mb.dlc.restored.message.id"))).getText();

            Assert.assertEquals(restoredMessageID, restoringMessageID, "Restoring messages of DeadLetter Channel is unsuccessful");
            log.info("Restoring messages of DeadLetter Channel is successful.");
        } else {
            Assert.fail("No messages in Queue" + qName + "after restoring");
        }

        //Testing reroute messages
        DLCBrowsePage dlcBrowsePage1 = homePage.getDLCBrowsePage();
        DLCContentPage dlcContentPage1 = dlcBrowsePage1.getDLCContent();
        reroutingMessageID = dlcContentPage1.rerouteFunction(rerouteQueue);
        QueuesBrowsePage queuesBrowsePage1 = homePage.getQueuesBrowsePage();
        QueueContentPage queueContentPage1 = queuesBrowsePage1.browseQueue(rerouteQueue);
        if (isElementPresent(UIElementMapper.getInstance()
                                     .getElement("mb.dlc.rerouted.queue.table"))) {
            reroutedMessageID = driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                                    .getElement("mb.dlc.rerouted.message.id"))).getText();
            Assert.assertEquals(reroutedMessageID, reroutingMessageID, "Rerouting messages of DeadLetter Channel is unsuccessful");
            log.info("Rerouting messages of dead letter channel is successful.");
        } else {
            Assert.fail("No messages in Queue" + rerouteQueue + "after rerouting");
        }

    }

    /**
     * Check whether element is present or not
     *
     * @param id - which element check for its availability
     * @return availability of the element
     */
    public boolean isElementPresent(String id) {
        return driver.findElements(By.xpath(id)).size() != 0;
    }

    /**
     * Search messageID through all messages in the queue
     *
     * @param deletingMessageID - Searching messageID
     * @param qName             - Searching queue
     * @return whether messageID available or not
     */
    public boolean checkMessages(String deletingMessageID, String qName) {
        boolean isSuccessful = true;
        if (isElementPresent(UIElementMapper.getInstance()
                                     .getElement("mb.dlc.browse.content.table"))) {
            WebElement queueTable = driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                                        .getElement("mb.dlc.browse.content.table")));
            List<WebElement> rowElementList = queueTable.findElements(By.tagName("tr"));
            // Go through table rows and find deleted messageID
            for (WebElement row : rowElementList) {
                List<WebElement> columnList = row.findElements(By.tagName("td"));
                // Assumption: there are eleven columns. MessageID is in second column
                if ((columnList.size() == COLUMN_LIST_SIZE) && columnList.get(MESSAGE_ID_COLUMN).getText().equals(deletingMessageID)) {
                    isSuccessful = false;
                    break;
                }
            }
        } else {
            Assert.fail("No messages in Queue" + qName + "after deleting");
        }
        return isSuccessful;
    }

    @AfterClass()
    public void tearDown() {
        //AndesAckWaitTimeOut set to default value.
        System.setProperty("AndesAckWaitTimeOut", "30000");
        driver.quit();
    }

}
