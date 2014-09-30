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

        /*automationContext1 = getAutomationContextWithKey("mb002");
        automationContext2 = getAutomationContextWithKey("mb003");

        andesAdminClient1 = new AndesAdminClient(automationContext1.getContextUrls().getBackEndUrl(),
                super.login(automationContext1), ConfigurationContextProvider.getInstance().getConfigurationContext());

        andesAdminClient2 = new AndesAdminClient(automationContext2.getContextUrls().getBackEndUrl(),
                super.login(automationContext2), ConfigurationContextProvider.getInstance().getConfigurationContext());
*/

    }

    @Test(groups = "wso2.mb", description = "Single queue Single node send-receive test case", enabled = false)
    public void testSingleQueueSingleNodeSendReceive() throws XPathExpressionException,
            AndesAdminServiceBrokerManagerAdminException, RemoteException {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        String randomInstanceKey = getRandomInstance(null);

        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        String hostinfo = tempContext.getInstance().getHosts().get("default") + ":" +
                tempContext.getInstance().getPorts().get("qpid");

        AndesClient receivingClient = new AndesClient("receive", hostinfo
                , "queue:singleQueue1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        randomInstanceKey = getRandomInstance(randomInstanceKey);
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

        if(receiveSuccess && sendSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }
        assertEquals((receiveSuccess && sendSuccess), true);
    }


    @Test(groups = "wso2.mb", description = "Single queue replication", enabled = true)
    public void testSingleQueueReplication() throws Exception {

        String queueName = "singleQueue2";
        boolean bQueueReplicated = false;

        String randomInstanceKey = getRandomInstance(null);

        AndesAdminClient tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);

        if (tempAndesAdminClient.getQueueByName(queueName) != null) {
            tempAndesAdminClient.deleteQueue(queueName);
        }

        tempAndesAdminClient.createQueue(queueName);

        randomInstanceKey = getRandomInstance(randomInstanceKey);
        tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);
        Queue queue = tempAndesAdminClient.getQueueByName(queueName);

        assertTrue(queue != null && queue.getQueueName().equalsIgnoreCase(queueName) ,
                "Queue created in MB node instance not replicated in other MB node instance");

        tempAndesAdminClient.deleteQueue(queueName);
        randomInstanceKey = getRandomInstance(randomInstanceKey);
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

        String randomInstanceKey = getRandomInstance(null);
        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        String hostinfo1 = tempContext.getInstance().getHosts().get("default") + ":" +
                tempContext.getInstance().getPorts().get("qpid");

        AndesClient receivingClient = new AndesClient("receive", hostinfo1
                , "queue:singleQueue3",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        randomInstanceKey = getRandomInstance(randomInstanceKey);
        tempContext = getAutomationContextWithKey(randomInstanceKey);

        String hostinfo2 = tempContext.getInstance().getHosts().get("default") + ":" +
                tempContext.getInstance().getPorts().get("qpid");

        AndesClient sendingClient = new AndesClient("send", hostinfo2
                , "queue:singleQueue3", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        if(receiveSuccess && sendSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        assertEquals((receiveSuccess && sendSuccess), true);
    }


    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        String randomInstanceKey = getRandomInstance(null);

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