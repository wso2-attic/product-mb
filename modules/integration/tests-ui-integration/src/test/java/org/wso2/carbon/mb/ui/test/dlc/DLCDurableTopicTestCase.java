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

package org.wso2.carbon.mb.ui.test.dlc;

import org.apache.commons.lang3.StringUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationUiBaseTest;
import org.wso2.mb.integration.common.utils.ui.UIElementMapper;
import org.wso2.mb.integration.common.utils.ui.pages.login.LoginPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.DLCBrowsePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.DLCContentPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.HomePage;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.List;

/**
 * This test case will test dead letter channel operations for durable topic messages.
 */
public class DLCDurableTopicTestCase extends MBIntegrationUiBaseTest {
    private static final int COLUMN_LIST_SIZE = 11;
    private static final int MESSAGE_ID_COLUMN = 1;
    private static final long SEND_COUNT = 2L;
    private static final long EXPECTED_COUNT = 2L;

    /**
     * DLC test queue name
     */
    private static final String DLC_TEST_DURABLE_TOPIC = "DLCTestQueue";

    /**
     * Andes consumer client
     */
    private AndesClient consumerClient = null;

    /**
     * The home page of MB management console
     */
    private HomePage homePage = null;

    /**
     * The default andes acknowledgement wait timeout.
     */
    private String defaultAndesAckWaitTimeOut = null;

    /**
     * Initializes test. This class will initialize web driver and
     * restart server with altered broker.xml
     *
     * @throws AutomationUtilException
     * @throws XPathExpressionException
     * @throws IOException
     */
    @BeforeClass()
    public void initialize() throws AutomationUtilException, XPathExpressionException, IOException {
        super.init();
        super.restartServerWithAlteredMaximumRedeliveryAttempts();
    }

    /**
     * Purge all messages in dlc before test starts using ui.
     *
     * @throws XPathExpressionException
     * @throws IOException
     */
    @BeforeMethod()
    public void cleanDeadLetterChannel() throws XPathExpressionException, IOException {
        driver.get(getLoginURL());
        LoginPage loginPage = new LoginPage(driver);
        homePage = loginPage.loginAs(mbServer.getContextTenant()
                                             .getContextUser().getUserName(), mbServer
                                             .getContextTenant().getContextUser()
                                             .getPassword());

        DLCBrowsePage dlcBrowsePage = homePage.getDLCBrowsePage();
        //Testing delete messages
        DLCContentPage dlcContentPage = dlcBrowsePage.getDLCContent();
        dlcContentPage.deleteAllDLCMessages();
    }

    /**
     * This method will add durable topic messages to dead letter channel.
     *
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @BeforeMethod(dependsOnMethods = {"cleanDeadLetterChannel"})
    public void addDurableTopicMessagesToDLC()
            throws AndesClientConfigurationException, NamingException,
                   JMSException, IOException, AndesClientException {
        // Get current "AndesAckWaitTimeOut" system property.
        defaultAndesAckWaitTimeOut = System.getProperty(AndesClientConstants.
                                                                ANDES_ACK_WAIT_TIMEOUT_PROPERTY);

        // Setting system property "AndesAckWaitTimeOut" for andes
        System.setProperty(AndesClientConstants.ANDES_ACK_WAIT_TIMEOUT_PROPERTY, "0");

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new
                AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, DLC_TEST_DURABLE_TOPIC);
        // Amount of message to receive
        consumerConfig.setDurable(true, DLC_TEST_DURABLE_TOPIC);
        consumerConfig.setSubscriptionID("durable-topic-sub-1");
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT + 200L);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE);
        consumerConfig.setAcknowledgeAfterEachMessageCount(EXPECTED_COUNT + 200L);

        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(ExchangeType.TOPIC, DLC_TEST_DURABLE_TOPIC);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        waitUntilMessagesReceived(6L);

        //Thread sleep until messages sent to DLC after breaching maximum number of retrying
        AndesClientUtils.sleepForInterval(80000L);

    }


    /**
     * This test will verify delete and restore functions in dlc for durable topic messages.
     * 1. delete first element in dlc table. if message id doesn't exist in dlc ui table after
     * delete test will be success.
     * 2. restore first element in dlc table. if consumer client receive new messages after restore
     * from dlc test will be success.
     * TODO: After reroute feature completed in ui there should be a test for reroute durable topic messages.
     *
     * @throws java.io.IOException
     */
    @Test()
    public void performDurableTopicDeadLetterChannelTestCase() throws IOException {

        String deletingMessageID;
        String restoringMessageID;

        DLCBrowsePage dlcBrowsePage = homePage.getDLCBrowsePage();
        Assert.assertNotNull(dlcBrowsePage.isDLCCreated(),
                             "DeadLetter Channel not created. " + DLC_TEST_DURABLE_TOPIC);
        //Testing delete messages
        DLCContentPage dlcContentPage = dlcBrowsePage.getDLCContent();
        deletingMessageID = dlcContentPage.deleteFunction();

        Assert.assertTrue(checkMessages(deletingMessageID, DLC_TEST_DURABLE_TOPIC),
                          "Deleting messages of dead letter channel is unsuccessful.");

        // number of messages received by consumer client before restore function triggered.
        Long beforeRestoreMessageReceivedCount = consumerClient.getReceivedMessageCount();

        //Testing restore messages
        restoringMessageID = dlcContentPage.restoreFunction();
        waitUntilMessagesReceived(9L);

        //Thread sleep until messages sent to DLC after breaching maximum number of retrying
        AndesClientUtils.sleepForInterval(80000L);

        // number of messages received by consumer client after restore function triggered.
        Long afterRestoreMessageReceivedCount = consumerClient.getReceivedMessageCount();

        // This assertion will check if consumer client has received messages messages after
        // restore function triggered from ui. If it receives messages after restore function triggered
        // this assertion will be success.
        Assert.assertTrue(beforeRestoreMessageReceivedCount < afterRestoreMessageReceivedCount,
                          restoringMessageID + " Durable topic message not successfully restored.");
    }

    /**
     * This method will wait until given number of messages received by the consumer client.
     *
     * @param numberOfMessages this will specify number of messages to wait before proceed
     *
     */
    public void waitUntilMessagesReceived(Long numberOfMessages) {


        while (true) {
            Long count = consumerClient.getReceivedMessageCount();
            if (count >= numberOfMessages) {
                break;
            }
            AndesClientUtils.sleepForInterval(1000L);
        }
    }


    /**
     * Check whether element is present or not
     *
     * @param id which element check for its availability
     * @return availability of the element
     */
    public boolean isElementPresent(String id) {
        return driver.findElements(By.xpath(id)).size() != 0;
    }

    /**
     * Search messageID through all messages in the queue
     *
     * @param deletingMessageID - Searching messageID
     * @param queueName             - Searching queue
     * @return whether messageID available or not
     */
    private boolean checkMessages(String deletingMessageID, String queueName) {
        boolean isSuccessful = true;
        if (isElementPresent(UIElementMapper.getInstance()
                                     .getElement("mb.dlc.browse.content.table"))) {
            WebElement queueTable = driver.findElement(By.xpath(UIElementMapper.getInstance().
                    getElement("mb.dlc.browse.content.table")));
            List<WebElement> rowElementList = queueTable.findElements(By.tagName("tr"));
            // Go through table rows and find deleted messageID
            for (WebElement row : rowElementList) {
                List<WebElement> columnList = row.findElements(By.tagName("td"));
                // Assumption: there are eleven columns. MessageID is in second column
                if ((COLUMN_LIST_SIZE == columnList.size()) && columnList.get(MESSAGE_ID_COLUMN)
                        .getText().equals(deletingMessageID)) {
                    isSuccessful = false;
                    break;
                }
            }
        } else {
            Assert.fail("No messages in Queue" + queueName + "after deleting");
        }
        return isSuccessful;
    }

    /**
     * This method will restore all the configurations back.
     * Following configurations will be restored.
     * 1. AndesAckWaitTimeOut system property.
     * 2. Restore default broker.xml and restart server.
     *
     * @throws IOException
     * @throws AutomationUtilException
     */
    @AfterClass()
    public void tearDown() throws IOException, AutomationUtilException {

        // Setting system property "AndesAckWaitTimeOut" to default value.
        if (StringUtils.isBlank(defaultAndesAckWaitTimeOut)) {
            System.clearProperty(AndesClientConstants.ANDES_ACK_WAIT_TIMEOUT_PROPERTY);
        } else {
            System.setProperty(AndesClientConstants.ANDES_ACK_WAIT_TIMEOUT_PROPERTY,
                               defaultAndesAckWaitTimeOut);
        }

        driver.quit();
        restartInPreviousConfiguration();
    }
}
