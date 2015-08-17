/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.carbon.mb.ui.test.dlc;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.authenticator.stub.LogoutAuthenticationExceptionException;
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;
import org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.clients.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;
import org.wso2.mb.integration.common.utils.backend.ConfigurationEditor;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationUiBaseTest;
import org.wso2.mb.integration.common.utils.ui.UIElementMapper;
import org.wso2.mb.integration.common.utils.ui.pages.login.LoginPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.DLCBrowsePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.DLCContentPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.HomePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueueAddPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueuesBrowsePage;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.util.List;

/**
 * This test case will test following 3 scenarios from ui which are currently
 * available in dlc browse page.
 *      Restore message from dlc.
 *      Deleting message from dlc.
 *      Rerouting message from dlc.
 */
public class DLCQueueTestCase extends MBIntegrationUiBaseTest {
    private static final Log log = LogFactory.getLog(DLCQueueTestCase.class);
    private static final int COLUMN_LIST_SIZE = 11;
    private static final int MESSAGE_ID_COLUMN = 1;
    private static final long SEND_COUNT = 15L;
    private static final long EXPECTED_COUNT = 15L;

    /**
     * DLC test queue name
     */
    private static final String DLC_TEST_QUEUE = "DLCTestQueue";

    /**
     * Initializes the test case and changes the number of delivery attempts of a message to 1.
     *
     * @throws AutomationUtilException
     * @throws XPathExpressionException
     * @throws MalformedURLException
     */
    @BeforeClass()
    public void initialize() throws AutomationUtilException, XPathExpressionException, IOException,
                                                                                                ConfigurationException {
        super.init();

        // Updating the redelivery attempts to 1 to speed up the test case.
        super.serverManager = new ServerConfigurationManager(mbServer);
        String defaultMBConfigurationPath = ServerConfigurationManager.getCarbonHome() + File.separator + "repository" +
                                            File.separator + "conf" + File.separator + "broker.xml";
        ConfigurationEditor configurationEditor = new ConfigurationEditor(defaultMBConfigurationPath);
        // Changing "maximumRedeliveryAttempts" value to "1" in broker.xml
        configurationEditor.updateProperty(AndesConfiguration.TRANSPORTS_AMQP_MAXIMUM_REDELIVERY_ATTEMPTS, "1");
        // Restarting server
        configurationEditor.applyUpdatedConfigurationAndRestartServer(serverManager);
    }

    /**
     * This test case will test restore,delete and reroute messages of
     * DeadLetter Channel from ui.
     * 1. Initially this test case will create a new queue to reroute messages.
     * 2. Delete queue message from dlc and check if message exist in dlc queue.
     * 3. Reroute queue message from dlc and check if queue message exist in browse queue ui.
     * 4. Reroute queue message from dlc and check if that queue message exist in reroute
     * browse queue ui.
     *
     * @throws XPathExpressionException
     * @throws IOException
     * @throws AndesAdminServiceBrokerManagerAdminException
     * @throws AndesClientConfigurationException
     * @throws JMSException
     * @throws NamingException
     * @throws AndesClientException
     * @throws AutomationUtilException
     * @throws LogoutAuthenticationExceptionException
     */
    @Test()
    public void performDeadLetterChannelTestCase() throws XPathExpressionException, IOException,
            AndesAdminServiceBrokerManagerAdminException, AndesClientConfigurationException, JMSException,
            NamingException, AndesClientException, AutomationUtilException, LogoutAuthenticationExceptionException {

        // Number of checks for an update in DLC message count.
        int tries = 15;

        // Getting message count in DLC prior adding new messages to DLC.
        long messageCountPriorSendingMessages = this.getDLCMessageCount();

        log.info("Message count in DLC before sending messages : " + messageCountPriorSendingMessages);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, DLC_TEST_QUEUE);
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT + 200L);
        consumerConfig.setAcknowledgeMode(JMSAcknowledgeMode.CLIENT_ACKNOWLEDGE);
        consumerConfig.setAcknowledgeAfterEachMessageCount(EXPECTED_COUNT + 210L);

        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, DLC_TEST_QUEUE);
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);

        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        // Waiting until the message count in DLC is different after the message were published.
        while (messageCountPriorSendingMessages == this.getDLCMessageCount()) {
            if(0 == tries){
                Assert.fail("Did not receive any message to DLC.");
            }
            // Reducing try count
            tries--;
            //Thread sleep until message count in DLC is changed
            AndesClientUtils.sleepForInterval(15000L);
            log.info("Waiting for message count change.");
        }

        log.info("Message count in DLC after sending messages : " + this.getDLCMessageCount());

        // Stops consuming messages
        consumerClient.stopClient();

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
                                                      .getContextTenant().getContextUser()
                                                      .getPassword());
        // Add an queue to test rerouting messages of DLC
        QueueAddPage queueAddPage = homePage.getQueueAddPage();
        Assert.assertEquals(queueAddPage.addQueue(rerouteQueue), true);
        DLCBrowsePage dlcBrowsePage = homePage.getDLCBrowsePage();
        Assert.assertNotNull(dlcBrowsePage.isDLCCreated(), "DeadLetter Channel not created. " + DLC_TEST_QUEUE);

        // Testing delete messages
        DLCContentPage dlcContentPage = dlcBrowsePage.getDLCContent();
        deletingMessageID = dlcContentPage.deleteFunction();

        Assert.assertTrue(checkMessages(deletingMessageID, DLC_TEST_QUEUE),
                          "Deleting messages of dead letter channel is unsuccessful.");

        // Testing restore messages
        restoringMessageID = dlcContentPage.restoreFunction();

        // Waiting till back end completes.
        AndesClientUtils.sleepForInterval(5000);

        QueuesBrowsePage queuesBrowsePage = homePage.getQueuesBrowsePage();
        queuesBrowsePage.browseQueue(DLC_TEST_QUEUE);
        if (isElementPresent(UIElementMapper.getInstance().getElement("mb.queue.browse.content.table"))) {
            restoredMessageID = driver.findElement(By.xpath(UIElementMapper.getInstance().
                                                                getElement("mb.dlc.restored.message.id"))).getText();

            Assert.assertEquals(restoredMessageID, restoringMessageID, "Restoring messages of DeadLetter Channel is " +
                                                                                                        "unsuccessful");
            log.info("Restoring messages of DeadLetter Channel is successful.");
        } else {
            Assert.fail("No messages in Queue" + DLC_TEST_QUEUE + "after restoring");
        }

        // Testing reroute messages
        DLCBrowsePage dlcBrowsePage1 = homePage.getDLCBrowsePage();
        DLCContentPage dlcContentPage1 = dlcBrowsePage1.getDLCContent();
        reroutingMessageID = dlcContentPage1.rerouteFunction(rerouteQueue);

        // Waiting till back end completes.
        AndesClientUtils.sleepForInterval(5000);
        QueuesBrowsePage queuesBrowsePage1 = homePage.getQueuesBrowsePage();
        queuesBrowsePage1.browseQueue(rerouteQueue);
        if (isElementPresent(UIElementMapper.getInstance().getElement("mb.dlc.rerouted.queue.table"))) {
            reroutedMessageID = driver.findElement(By.xpath(UIElementMapper.getInstance().
                                                   getElement("mb.dlc.rerouted.message.id"))).getText();
            Assert.assertEquals(reroutedMessageID, reroutingMessageID, "Rerouting messages of DeadLetter Channel is " +
                                                                                                        "unsuccessful");
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
            WebElement queueTable = driver.findElement(By.xpath(UIElementMapper.getInstance().
                                                       getElement("mb.dlc.browse.content.table")));
            List<WebElement> rowElementList = queueTable.findElements(By.tagName("tr"));
            // Go through table rows and find deleted messageID
            for (WebElement row : rowElementList) {
                List<WebElement> columnList = row.findElements(By.tagName("td"));
                // Assumption: there are eleven columns. MessageID is in second column
                if ((columnList.size() == COLUMN_LIST_SIZE) && columnList.get(MESSAGE_ID_COLUMN)
                        .getText().equals(deletingMessageID)) {
                    isSuccessful = false;
                    break;
                }
            }
        } else {
            Assert.fail("No messages in Queue" + qName + "after deleting");
        }
        return isSuccessful;
    }

    /**
     * This class will restore andes acknowledgement time out system property
     * and quit the ui web driver.
     *
     * @throws IOException
     * @throws AutomationUtilException
     * @throws LogoutAuthenticationExceptionException
     */
    @AfterClass()
    public void tearDown() throws IOException, AutomationUtilException, LogoutAuthenticationExceptionException {
        //Revert back to original configuration.
        super.serverManager.restoreToLastConfiguration(true);
        driver.quit();
    }

    /**
     * Gets the number of messages in the DLC queue.
     *
     * @return The number of messages.
     * @throws AutomationUtilException
     * @throws RemoteException
     * @throws LogoutAuthenticationExceptionException
     * @throws AndesAdminServiceBrokerManagerAdminException
     */
    private long getDLCMessageCount() throws AutomationUtilException, RemoteException,
            LogoutAuthenticationExceptionException, AndesAdminServiceBrokerManagerAdminException {
        LoginLogoutClient loginLogoutClientForAdmin = new LoginLogoutClient(mbServer);
        String sessionCookie = loginLogoutClientForAdmin.login();
        AndesAdminClient andesAdminClient = new AndesAdminClient(backendURL, sessionCookie);
        long messageCount = andesAdminClient.getDlcQueue().getMessageCount();
        loginLogoutClientForAdmin.logout();

        return messageCount;
    }
}
