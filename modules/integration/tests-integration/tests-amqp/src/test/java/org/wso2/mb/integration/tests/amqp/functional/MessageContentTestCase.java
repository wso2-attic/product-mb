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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * This class contains tests for message content validity.
 */
public class MessageContentTestCase extends MBIntegrationBaseTest {

    /**
     * File path to read message content for publishing
     */
    private static final String messageContentInputFilePath = System.getProperty("framework.resource.location") + File.separator +
                                                              "MessageContentInput.txt";

    /**
     * 300KB size. This is to create more than 3 message content chunks to check chunk data retrieval.
     */
    private static final int SIZE_TO_READ = 300 * 1024;
    /**
     * Message sent count.
     */
    private static final long SEND_COUNT = 1L;

    /**
     * Message expected count.
     */
    private static final long EXPECTED_COUNT = SEND_COUNT;

    /**
     * Initialize the test as super tenant user.
     *
     * @throws XPathExpressionException
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws XPathExpressionException {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * Test the message content integrity of a single message by comparing the sent and received message content
     * which spreads over several message content chunks.
     */
    @Test(groups = "wso2.mb", description = "Message content validation test case")
    public void performQueueContentSendReceiveTestCase()
            throws AndesClientConfigurationException, IOException, JMSException, NamingException {

        // Reading message content
        char[] inputContent = new char[SIZE_TO_READ];
        try {
            BufferedReader inputFileReader = new BufferedReader(new FileReader(messageContentInputFilePath));
            inputFileReader.read(inputContent);
        } catch (FileNotFoundException e) {
            log.warn("Error locating input content from file : " + messageContentInputFilePath);
        } catch (IOException e) {
            log.warn("Error reading input content from file : " + messageContentInputFilePath);
        }


        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "QueueContentSendReceive");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);
        consumerConfig.setFilePathToWriteReceivedMessages(AndesClientConstants.FILE_PATH_TO_WRITE_RECEIVED_MESSAGES); // writing received messages.

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "QueueContentSendReceive");
        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setReadMessagesFromFilePath(messageContentInputFilePath); // message content will be read from this path and published

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();

        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Reading received message content
        char[] outputContent = new char[SIZE_TO_READ];

        try {
            BufferedReader inputFileReader = new BufferedReader(new FileReader(AndesClientConstants.FILE_PATH_TO_WRITE_RECEIVED_MESSAGES));
            inputFileReader.read(outputContent);
        } catch (FileNotFoundException e) {
            log.warn("Error locating output content from file : " + messageContentInputFilePath);
        } catch (IOException e) {
            log.warn("Error reading output content from file : " + messageContentInputFilePath);
        }

        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed.");
        Assert.assertEquals(new String(outputContent), new String(inputContent), "Message content has been modified.");
    }
}
