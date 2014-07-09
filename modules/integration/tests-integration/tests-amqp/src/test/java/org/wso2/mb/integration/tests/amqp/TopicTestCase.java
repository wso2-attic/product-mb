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

package org.wso2.mb.integration.tests.amqp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TopicTestCase extends MBIntegrationBaseTest{
    private static final Log log = LogFactory.getLog(TopicTestCase.class);

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = "wso2.mb", description = "Single topic send-receive test case")
    public void performSingleTopicSendReceiveTestCase() {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:singleTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:singleTopic", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        if(receiveSuccess && sendSuccess) {
            log.info("TEST PASSED");
        }  else {
            log.info("TEST FAILED");
        }
        assertEquals((receiveSuccess && sendSuccess), true);
    }

    @Test(groups = "wso2.mb", description = "")
    public void perform(){
        int sendMessageCount = 100;
        int runTime = 20;
        int expectedMessageCount = 100;

        // Start receiving clients (tenant1, tenant2 and admin)
        AndesClient tenant1ReceivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:commontopic",
                "100", "false", Integer.toString(runTime),Integer.toString(expectedMessageCount),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedMessageCount, "",
                "tenant1user1!testtenant1.com", "tenant1user1");
        tenant1ReceivingClient.startWorking();

        AndesClient tenant2ReceivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:commontopic",
                "100", "false", Integer.toString(runTime),Integer.toString(expectedMessageCount),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedMessageCount, "",
                "tenant2user1!testtenant2.com", "tenant2user1");
        tenant2ReceivingClient.startWorking();

        AndesClient adminReceivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:commontopic",
                "100", "false", Integer.toString(runTime), Integer.toString(expectedMessageCount),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedMessageCount, "");
        adminReceivingClient.startWorking();

        // Start sending clients (tenant1, tenant2 and admin)
        AndesClient tenant1SendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:commontopic",
                "100", "false", Integer.toString(runTime), Integer.toString(sendMessageCount), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendMessageCount, "",
                "tenant1user1!testtenant1.com", "tenant1user1");

        AndesClient tenant2SendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:commontopic",
                "100", "false", Integer.toString(runTime), Integer.toString(sendMessageCount), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendMessageCount, "",
                "tenant2user1!testtenant2.com", "tenant2user1");

        AndesClient adminSendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:commontopic",
                "100", "false", Integer.toString(runTime), Integer.toString(sendMessageCount), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendMessageCount, "");

        tenant1SendingClient.startWorking();
        tenant2SendingClient.startWorking();
        adminSendingClient.startWorking();

        boolean tenet1ReceiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(tenant1ReceivingClient, expectedMessageCount, runTime);
        boolean tenet2ReceiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(tenant2ReceivingClient, expectedMessageCount, runTime);
        boolean adminReceiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(adminReceivingClient, expectedMessageCount, runTime);

        boolean tenant1SendSuccess = AndesClientUtils.getIfSenderIsSuccess(tenant1SendingClient,sendMessageCount);
        boolean tenant2SendSuccess = AndesClientUtils.getIfSenderIsSuccess(tenant2SendingClient,sendMessageCount);
        boolean adminSendSuccess = AndesClientUtils.getIfSenderIsSuccess(adminSendingClient,sendMessageCount);

        assertTrue(tenant1SendSuccess , "TENANT 1 SENT SUCCESSFULLY");
        assertTrue(tenant2SendSuccess , "TENANT 2 SENT SUCCESSFULLY");
        assertTrue(adminSendSuccess , "ADMIN SENT SUCCESSFULLY");
        assertTrue(tenet1ReceiveSuccess, "TENANT 1 RECEIVED SUCCESSFULLY");
        assertTrue(tenet2ReceiveSuccess, "TENANT 2 RECEIVED SUCCESSFULLY");
        assertTrue(adminReceiveSuccess, "ADMIN RECEIVED SUCCESSFULLY");
    }
}
