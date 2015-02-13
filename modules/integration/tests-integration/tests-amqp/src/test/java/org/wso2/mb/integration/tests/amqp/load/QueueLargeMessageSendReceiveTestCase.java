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


/**
 * Send large messages (1MB and 10MB) to message broker and check if they are received
 */
package org.wso2.mb.integration.tests.amqp.load;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import java.io.*;

import static org.testng.Assert.assertEquals;

public class QueueLargeMessageSendReceiveTestCase extends MBIntegrationBaseTest {

    // Input file to read a 1MB message content.
    String messageContentInputFilePath = System.getProperty("framework.resource.location") + File.separator +
            "MessageContentInput.txt";

    // 1MB size.
    public static final int SIZE_TO_READ = 1024 * 1024;

    /**
     * Initialize the test as super tenant user.
     *
     * @throws Exception
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * check with 1MB messages
     */
    @Test(groups = {"wso2.mb", "queue"})
    public void performQueueOneMBSizeMessageSendReceiveTestCase() {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = sendCount;

        String queueNameArg = "queue:Queue1MBSendReceive";

        char[] inputContent = new char[SIZE_TO_READ];

        try {
            BufferedReader inputFileReader = new BufferedReader(new FileReader(messageContentInputFilePath));
            inputFileReader.read(inputContent);
        } catch (FileNotFoundException e) {
            log.warn("Error locating input content from file : " + messageContentInputFilePath);
        } catch (IOException e) {
            log.warn("Error reading input content from file : " + messageContentInputFilePath);
        }

        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", queueNameArg,
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", queueNameArg, "100", "true",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,file=" + messageContentInputFilePath + ",stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, sendCount, runTime);

        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);

        Integer actualReceiveCount = receivingClient.getReceivedqueueMessagecount();

        Assert.assertTrue(sendSuccess, "Message sending failed.");
        Assert.assertTrue(receiveSuccess, "Message receiving failed.");

        assertEquals(actualReceiveCount, sendCount, "Did not receive expected message count.");
    }

    /**
     * check with 10MB messages
     */
    @Test(groups = {"wso2.mb", "queue"}, enabled = false)
    public void performQueueTenMBSizeMessageSendReceiveTestCase() {
        Integer sendCount = 10;
        Integer runTime = 120;
        Integer expectedCount = 10;
        String pathOfSampleFileToReadContent = System.getProperty("resources.dir") + File.separator + "sample.xml";
        String pathOfFileToReadContent = System.getProperty("resources.dir") + File.separator + "pom10mb.xml";
        AndesClientUtils.createTestFileToSend(pathOfSampleFileToReadContent, pathOfFileToReadContent, 10 * 1024);

        AndesClientTemp receivingClient = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:singleLargeQueue10MB",
                "1", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClientTemp sendingClient = new AndesClientTemp("send", "127.0.0.1:5672", "queue:singleLargeQueue10MB", "1",
                "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,file=" + pathOfFileToReadContent + ",delayBetweenMsg=0,stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingClient, sendCount);

        Assert.assertTrue(sendSuccess, "Message sending failed.");
        Assert.assertTrue(receiveSuccess, "Message receiving failed.");
    }


}
