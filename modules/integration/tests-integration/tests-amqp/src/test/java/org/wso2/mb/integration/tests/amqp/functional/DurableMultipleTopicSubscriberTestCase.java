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

import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import static org.testng.Assert.assertTrue;

/**
 * 1. start two durable topic subscription
 * 2. send 1500 messages
 */
public class DurableMultipleTopicSubscriberTestCase {

    @Test(groups = {"wso2.mb", "durableTopic"})
    public void performDurableTopicTestCase() {

        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 500;

        // Start subscription 1
        AndesClient receivingClient1 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=subwso2,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");
        receivingClient1.startWorking();

        // Start subscription 2
        AndesClient receivingClient2 = new AndesClient("receive", "127.0.0.1:5672", "topic:durableTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,durable=true,subscriptionID=wso2sub,delayBetweenMsg=0," +
                "stopAfter=" + expectedCount, "");
        receivingClient2.startWorking();

        // Start message publisher
        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:durableTopic", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, "");
        sendingClient.startWorking();

        AndesClientUtils.sleepForInterval(2000);

        boolean receivingSuccess1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient1, expectedCount,
                runTime);
        assertTrue(receivingSuccess1, "Message receive error from subscriber 1");

        boolean receivingSuccess2 = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient2, expectedCount,
                runTime);
        assertTrue(receivingSuccess2, "Message receive error from subscriber 2");

        AndesClientUtils.sleepForInterval(2000);

        boolean sendingSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);
        assertTrue(sendingSuccess, "Message send error");
    }
}
