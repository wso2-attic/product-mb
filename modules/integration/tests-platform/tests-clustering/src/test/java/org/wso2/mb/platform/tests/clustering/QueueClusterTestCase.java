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
    AutomationContext automationContext1;
    AutomationContext automationContext2;
    private AndesAdminClient andesAdminClient1;
    private AndesAdminClient andesAdminClient2;



    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {

        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);
        super.initAndesAdminClients();
    }

    @Test(groups = "wso2.mb", description = "Single queue Single node send-receive test case", enabled = false)
    public void testSingleQueueSingleNodeSendReceive() throws XPathExpressionException,
            AndesAdminServiceBrokerManagerAdminException, RemoteException {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        String randomInstanceKey = getRandomMBInstance();

        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        String hostinfo = tempContext.getInstance().getHosts().get("default") + ":" +
                tempContext.getInstance().getPorts().get("qpid");

        AndesClient receivingClient = new AndesClient("receive", hostinfo
                , "queue:singleQueue1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        randomInstanceKey = getRandomMBInstance();
        boolean bQueueReplicated = false;
        Queue queue = getAndesAdminClientWithKey(randomInstanceKey).getQueueByName("singleQueue1");

        assertTrue(queue.getQueueName().equalsIgnoreCase("singleQueue1"), "Queue created in MB node 1 not exist");

        AndesClient sendingClient = new AndesClient("send", hostinfo
                , "queue:singleQueue1", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        assertEquals((receiveSuccess && sendSuccess), true);
    }


    @Test(groups = "wso2.mb", description = "Single queue replication", enabled = true)
    public void testSingleQueueReplication() throws Exception {

        String queueName = "singleQueue2";
        boolean bQueueReplicated = false;

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

    @Test(groups = "wso2.mb", description = "Single queue Multi node send-receive test case", enabled = false)
    public void testSingleQueueMultiNodeSendReceive() throws XPathExpressionException,
            AndesAdminServiceBrokerManagerAdminException, RemoteException {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        String randomInstanceKey = getRandomMBInstance();
        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        String hostinfo1 = tempContext.getInstance().getHosts().get("default") + ":" +
                tempContext.getInstance().getPorts().get("qpid");

        AndesClient receivingClient = new AndesClient("receive", hostinfo1
                , "queue:singleQueue3",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        randomInstanceKey = getRandomMBInstance();
        tempContext = getAutomationContextWithKey(randomInstanceKey);

        String hostInfo2 = tempContext.getInstance().getHosts().get("default") + ":" +
                tempContext.getInstance().getPorts().get("qpid");

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