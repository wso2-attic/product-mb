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
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import java.io.*;

/**
 * This class contains tests for message content validity.
 */
public class MessageContentTestCase extends MBIntegrationBaseTest {

    String messageContentInputFilePath = System.getProperty("framework.resource.location") + File.separator +
            "MessageContentInput.txt";

    // 300KB size. This is to create more than 3 message content chunks to check chunk data retrieval.
    public static final int SIZE_TO_READ = 300 * 1024;

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
     * Test the message content integrity of a single message by comparing the sent and received message content
     * which spreads over several message content chunks.
     */
    @Test(groups = "wso2.mb", description = "Message content validation test case")
    public void performQueueContentSendReceiveTestCase() {
        Integer sendCount = 1;
        Integer runTime = 20;
        Integer expectedCount = 1;
        String queueNameArg = "queue:QueueContentSendReceive";

        char[] inputContent = new char[SIZE_TO_READ];

        try {
            BufferedReader inputFileReader = new BufferedReader(new FileReader(messageContentInputFilePath));
            inputFileReader.read(inputContent);
        } catch (FileNotFoundException e) {
            log.warn("Error locating input content from file : " + messageContentInputFilePath);
        } catch (IOException e) {
            log.warn("Error reading input content from file : " + messageContentInputFilePath);
        }

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", queueNameArg,
                "100", "true", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", queueNameArg, "100", "true",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,file=" + messageContentInputFilePath + ",stopAfter=" + sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        char[] outputContent = new char[SIZE_TO_READ];

        try {
            BufferedReader inputFileReader = new BufferedReader(new FileReader(receivingClient
                    .filePathToWriteReceivedMessages));
            inputFileReader.read(outputContent);
        } catch (FileNotFoundException e) {
            log.warn("Error locating output content from file : " + messageContentInputFilePath);
        } catch (IOException e) {
            log.warn("Error reading output content from file : " + messageContentInputFilePath);
        }

        Assert.assertTrue(sendSuccess, "Message sending failed.");
        Assert.assertTrue(receiveSuccess, "Message receiving failed.");

        Assert.assertEquals(new String(outputContent), new String(inputContent), "Message content has been modified.");
    }
}
