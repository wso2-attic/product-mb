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

package org.wso2.mb.integration.tests.amqp.load;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;


/**
 * Send large messages (1MB and 10MB) to message broker and check if they are received
 */
public class TopicLargeMessagePublishConsumeTestCase {

    @BeforeClass
    public void prepare() {
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * check with 1MB messages
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void performTopicOneMBSizeMessageSendReceiveTestCase()
            throws AndesClientException, IOException, NamingException, JMSException {
        int sendCount = 10;

        String pathOfSampleFileToReadContent = System.getProperty("resources.dir") + File.separator + "sample.xml";
        String pathOfFileToReadContent = System.getProperty("resources.dir") + File.separator + "pom1mb.xml";
        AndesClientUtils.createTestFileToSend(pathOfSampleFileToReadContent, pathOfFileToReadContent, 1024);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "singleLargeTopic1MB");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(sendCount);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "singleLargeTopic1MB");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);
        publisherConfig.setReadMessagesFromFilePath(pathOfFileToReadContent);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), sendCount, "Message receiving failed.");





//        Integer sendCount = 10;
//        Integer runTime = 120;
//        Integer expectedCount = 10;
//        String pathOfSampleFileToReadContent = System.getProperty("resources.dir") + File.separator + "sample.xml";
//        String pathOfFileToReadContent = System.getProperty("resources.dir") + File.separator + "pom1mb.xml";
//        AndesClientUtils.createTestFileToSend(pathOfSampleFileToReadContent, pathOfFileToReadContent, 1024);
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:singleLargeTopic1MB",
//                "1", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:singleLargeTopic1MB", "1", "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,file=" + pathOfFileToReadContent + ",delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(sendSuccess, "Message sending failed.");
//        Assert.assertTrue(receiveSuccess, "Message receiving failed.");
    }

    /**
     * check with 10MB messages
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void performTopicTenMBSizeMessageSendReceiveTestCase()
            throws AndesClientException, NamingException, JMSException, IOException {
        int sendCount = 10;

        String pathOfSampleFileToReadContent = System.getProperty("resources.dir") + File.separator + "sample.xml";
        String pathOfFileToReadContent = System.getProperty("resources.dir") + File.separator + "pom1mb.xml";
        AndesClientUtils.createTestFileToSend(pathOfSampleFileToReadContent, pathOfFileToReadContent, 1024);

        // Creating a initial JMS consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "singleLargeTopic10MB");
        // Amount of message to receive
        consumerConfig.setMaximumMessagesToReceived(sendCount);
        // Prints per message
        consumerConfig.setPrintsPerMessageCount(sendCount / 10L);

        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "singleLargeTopic10MB");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);
        publisherConfig.setReadMessagesFromFilePath(pathOfFileToReadContent);

        AndesClient consumerClient = new AndesClient(consumerConfig);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig);
        publisherClient.startClient();

        AndesClientUtils.waitUntilNoMessagesAreReceivedAndShutdownClients(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Message sending failed");
        Assert.assertEquals(consumerClient.getReceivedMessageCount(), sendCount, "Message receiving failed.");



//        Integer sendCount = 10;
//        Integer runTime = 120;
//        Integer expectedCount = 10;
//        String pathOfSampleFileToReadContent = System.getProperty("resources.dir") + File.separator + "sample.xml";
//        String pathOfFileToReadContent = System.getProperty("resources.dir") + File.separator + "pom10mb.xml";
//        AndesClientUtils.createTestFileToSend(pathOfSampleFileToReadContent, pathOfFileToReadContent, 10 * 1024);
//
//        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "topic:singleLargeTopic10MB",
//                "1", "false", runTime.toString(), expectedCount.toString(),
//                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");
//
//        receivingClient.startWorking();
//
//        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "topic:singleLargeTopic10MB", "1",
//                "false",
//                runTime.toString(), sendCount.toString(), "1",
//                "ackMode=1,file=" + pathOfFileToReadContent + ",delayBetweenMsg=0,stopAfter=" + sendCount, "");
//
//        sendingClient.startWorking();
//
//        boolean receiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
//
//        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);
//
//        Assert.assertTrue(sendSuccess, "Message sending failed.");
//        Assert.assertTrue(receiveSuccess, "Message receiving failed.");
    }
}
