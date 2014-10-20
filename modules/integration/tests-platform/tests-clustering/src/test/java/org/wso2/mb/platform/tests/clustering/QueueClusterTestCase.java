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
 * under the License. and limitations under the License.
 */

package org.wso2.mb.platform.tests.clustering;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.andes.stub.admin.types.Queue;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.queue.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import javax.xml.xpath.XPathExpressionException;
import java.rmi.RemoteException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class QueueClusterTestCase extends MBPlatformBaseTest {

    private static final Log log = LogFactory.getLog(QueueClusterTestCase.class);

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {

        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);
        super.initAndesAdminClients();
    }

    @Test(groups = "wso2.mb", description = "Single queue Single node send-receive test case")
    public void testSingleQueueSingleNodeSendReceive() throws XPathExpressionException,
            AndesAdminServiceBrokerManagerAdminException, RemoteException {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        String randomInstanceKey = getRandomMBInstance();

        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        String hostInfo = tempContext.getInstance().getHosts().get("default") + ":" +
                tempContext.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostInfo
                , "queue:singleQueue1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        randomInstanceKey = getRandomMBInstance();

        Queue queue = getAndesAdminClientWithKey(randomInstanceKey).getQueueByName("singleQueue1");

        assertTrue(queue.getQueueName().equalsIgnoreCase("singleQueue1"), "Queue created in MB node 1 not exist");

        AndesClient sendingClient = new AndesClient("send", hostInfo
                , "queue:singleQueue1", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }


    @Test(groups = "wso2.mb", description = "Single queue replication")
    public void testSingleQueueReplication() throws Exception {

        String queueName = "singleQueue2";

        String randomInstanceKey = getRandomMBInstance();

        AndesAdminClient tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);

        if (tempAndesAdminClient.getQueueByName(queueName) != null) {
            tempAndesAdminClient.deleteQueue(queueName);
        }

        tempAndesAdminClient.createQueue(queueName);

        randomInstanceKey = getRandomMBInstance();
        tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);
        Queue queue = tempAndesAdminClient.getQueueByName(queueName);

        assertTrue(queue != null && queue.getQueueName().equalsIgnoreCase(queueName) ,
                "Queue created in MB node instance not replicated in other MB node instance");

        tempAndesAdminClient.deleteQueue(queueName);
        randomInstanceKey = getRandomMBInstance();
        tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);
        queue = tempAndesAdminClient.getQueueByName(queueName);

        assertTrue(queue == null,
                "Queue created in MB node instance not replicated in other MB node instance");

    }

    @Test(groups = "wso2.mb", description = "Single queue Multi node send-receive test case")
    public void testSingleQueueMultiNodeSendReceive() throws XPathExpressionException,
            AndesAdminServiceBrokerManagerAdminException, RemoteException {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        String randomInstanceKey = getRandomMBInstance();
        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        String hostInfo1 = tempContext.getInstance().getHosts().get("default") + ":" +
                tempContext.getInstance().getPorts().get("amqp");

        AndesClient receivingClient = new AndesClient("receive", hostInfo1
                , "queue:singleQueue3",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        randomInstanceKey = getRandomMBInstance();
        tempContext = getAutomationContextWithKey(randomInstanceKey);

        String hostInfo2 = tempContext.getInstance().getHosts().get("default") + ":" +
                tempContext.getInstance().getPorts().get("amqp");

        AndesClient sendingClient = new AndesClient("send", hostInfo2
                , "queue:singleQueue3", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }


    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        String randomInstanceKey = getRandomMBInstance();

        AndesAdminClient tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);

        if (tempAndesAdminClient.getQueueByName("singleQueue1") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue1");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueue2") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue2");
        }

        if (tempAndesAdminClient.getQueueByName("singleQueue3") != null) {
            tempAndesAdminClient.deleteQueue("singleQueue3");
        }
    }

}
