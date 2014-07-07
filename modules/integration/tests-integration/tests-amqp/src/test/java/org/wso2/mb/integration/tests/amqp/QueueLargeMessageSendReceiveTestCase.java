/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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


/**
 * Send large messages (1MB and 10MB) to message broker and check if they are received
 */
package org.wso2.mb.integration.tests.amqp;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import java.io.*;

public class QueueLargeMessageSendReceiveTestCase {


    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * check with 1MB messages
     */
    @Test(groups = {"wso2.mb", "queue"})
    public void performQueueOneMBSizeMessageSendReceiveTestCase() {
        Integer sendCount = 10;
        Integer runTime = 120;
        Integer expectedCount = 10;
        String pathOfSampleFileToReadContent = System.getProperty("resources.dir") + File.separator +"sample.xml";
        String pathOfFileToReadContent = System.getProperty("resources.dir") + File.separator +"pom1mb.xml";
        AndesClientUtils.createTestFileToSend(pathOfSampleFileToReadContent,pathOfFileToReadContent,1024);

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:singleLargeQueue1MB",
                "1", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:singleLargeQueue1MB", "1", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,file="+ pathOfFileToReadContent +",delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        if(receiveSuccess && sendSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals((receiveSuccess && sendSuccess), true);
    }

    /**
     * check with 10MB messages
     */
    @Test(groups = {"wso2.mb", "queue"})
    public void performQueueTenMBSizeMessageSendReceiveTestCase() {
        Integer sendCount = 10;
        Integer runTime = 120;
        Integer expectedCount = 10;
        String pathOfSampleFileToReadContent = System.getProperty("resources.dir") + File.separator +"sample.xml";
        String pathOfFileToReadContent = System.getProperty("resources.dir") + File.separator +"pom10mb.xml";
        AndesClientUtils.createTestFileToSend(pathOfSampleFileToReadContent,pathOfFileToReadContent,10*1024);

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:singleLargeQueue10MB",
                "1", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:singleLargeQueue10MB", "1", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,file=" + pathOfFileToReadContent + ",delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        if(receiveSuccess && sendSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals((receiveSuccess && sendSuccess), true);
    }


}
