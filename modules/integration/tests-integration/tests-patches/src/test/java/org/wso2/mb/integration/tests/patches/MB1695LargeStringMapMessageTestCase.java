/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 *
 */

package org.wso2.mb.integration.tests.patches;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This class contains tests for message content validity.
 */
public class MB1695LargeStringMapMessageTestCase extends MBIntegrationBaseTest {

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
        super.init(TestUserMode.SUPER_TENANT_ADMIN);
    }

    /**
     * Test if Map messages containing multiple entries with 250K String sizes can be sent and received.
     *
     * @throws AndesClientConfigurationException
     * @throws IOException
     * @throws JMSException
     * @throws NamingException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "Message content validation test case")
    public void performLargeStringMapMessageSendReceiveTestCase()
            throws AndesClientConfigurationException, IOException, JMSException, NamingException,
            AndesClientException, XPathExpressionException {

        String queueName = "LargeMapMessageQueue";
        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(getAMQPPort(), ExchangeType.QUEUE, queueName);
        consumerConfig.setMaximumMessagesToReceived(EXPECTED_COUNT);

        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(getAMQPPort(), ExchangeType.QUEUE, queueName);

        publisherConfig.setNumberOfMessagesToSend(SEND_COUNT);
        publisherConfig.setJMSMessageType(JMSMessageType.MAP);

        //Write large map message to input file
        String filePath =
                System.getProperty("framework.resource.location") + File.separator + "MapMessageContentInput.txt";
        BufferedWriter br = new BufferedWriter(new FileWriter(new File((filePath))));
        for (int i = 0; i < 3; i++) {
            StringBuilder builder = new StringBuilder("");
            for (int j = 0; j < 250000; j++) {
                builder.append("a");
            }
            br.write(builder.toString());
            br.newLine();
        }
        br.close();

        // message content will be read from this path and published
        publisherConfig.setReadMessagesFromFilePath(filePath);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();
        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), SEND_COUNT, "Message sending failed.");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), EXPECTED_COUNT, "Message receiving failed.");
    }
}