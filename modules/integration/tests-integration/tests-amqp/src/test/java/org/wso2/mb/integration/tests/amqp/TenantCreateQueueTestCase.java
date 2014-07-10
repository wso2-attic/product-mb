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

import org.wso2.carbon.integration.common.admin.client.LogViewerClient;
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;
import org.wso2.carbon.logging.view.stub.types.carbon.LogEvent;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import java.rmi.RemoteException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TenantCreateQueueTestCase extends MBIntegrationBaseTest {
    private static final Log log = LogFactory.getLog(TenantCreateQueueTestCase.class);

    private static final String ERROR_STRING = "ERROR {org.wso2.carbon.context.internal.CarbonContextDataHolder} - Trying to set the domain from testtenant1.com to carbon.super";
    private static final String INFO_STRING = "INFO {org.wso2.andes.messageStore.CQLBasedMessageStoreImpl}";

    private LogViewerClient logViewerClient;

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        LoginLogoutClient loginLogoutClient = new LoginLogoutClient(automationContext);
        String sessionCookie = loginLogoutClient.login();
        logViewerClient =  new LogViewerClient(automationContext.getContextUrls().getBackEndUrl(),sessionCookie);
    }

    @Test(groups = "wso2.mb", description = "Single queue send-receive test case")
    public void performSingleQueueSendReceiveTestCase() throws RemoteException {
        int sendMessageCount = 100;
        int runTime = 20;
        int expectedMessageCount = 100;

        // Start receiving clients (tenant1, tenant2 and admin)
        AndesClient tenant1ReceivingClient = new AndesClient("receive", "127.0.0.1:5672", "queue:testtenant1.com/www",
                "100", "false", Integer.toString(runTime), Integer.toString(expectedMessageCount),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedMessageCount, "",
                "tenant1user1!testtenant1.com", "tenant1user1");
        tenant1ReceivingClient.startWorking();

        // Start sending clients (tenant1, tenant2 and admin)
        AndesClient tenant1SendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:testtenant1.com/www",
                "100", "false", Integer.toString(runTime), Integer.toString(sendMessageCount), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendMessageCount, "",
                "tenant1user1!testtenant1.com", "tenant1user1");

        tenant1SendingClient.startWorking();
        AndesClientUtils.sleepForInterval(10000);

        boolean tenet1ReceiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(tenant1ReceivingClient, expectedMessageCount, runTime);

        boolean tenant1SendSuccess = AndesClientUtils.getIfSenderIsSuccess(tenant1SendingClient, sendMessageCount);

        assertTrue(tenant1SendSuccess, "TENANT 1 send failed");
        assertTrue(tenet1ReceiveSuccess, "TENANT 1 receive failed");

        LogEvent[] logEvents = logViewerClient.getAllSystemLogs();

        for (LogEvent event : logEvents) {
            if (event.isPrioritySpecified()) {
                assertFalse(event.getPriority().contains("ERROR"), "ERROR occured in sever" + event.getMessage());
            }
        }
    }
}