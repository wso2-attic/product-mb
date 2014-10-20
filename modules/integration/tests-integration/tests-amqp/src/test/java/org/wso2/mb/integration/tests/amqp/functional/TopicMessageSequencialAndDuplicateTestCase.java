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
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import java.util.Map;

/**
 * 1. Send messages to a single topic and receive them
 * 2. listen for some more time to see if there are duplicates coming
 * 3. check if messages were received in order
 * 4. check if messages have any duplicates
 */
public class TopicMessageSequencialAndDuplicateTestCase {


    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "topic"})
    public void performTopicMessageSequencialAndDuplicateTestCase() {

        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 5000;


        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:singleTopic",
                "100", "true", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:singleTopic", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean success = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean senderSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        boolean receiveSuccess = false;
        if(receivingClient.getReceivedTopicMessagecount() == sendCount) {
            receiveSuccess = true;
        } else {
            receiveSuccess = false;
        }

        boolean isMessagesAreInOrder = receivingClient.checkIfMessagesAreInOrder();
        if(isMessagesAreInOrder) {
            System.out.println("***Messages Are In Order");
        }

        Map<Long, Integer> duplicateMessages = receivingClient.checkIfMessagesAreDuplicated();
        if(duplicateMessages.keySet().size() == 0) {
            System.out.println("*****Messages Are Not Duplicated");
        }

        if(senderSuccess && receiveSuccess && isMessagesAreInOrder && duplicateMessages.keySet().size() == 0) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals((senderSuccess && receiveSuccess && isMessagesAreInOrder && duplicateMessages.keySet().size() == 0), true);
    }
}
