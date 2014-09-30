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
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.queue.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;
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
        super.initCluster(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);

        System.out.println("\n\n\n\n\nSTART: ");
        super.initCluster(TestUserMode.SUPER_TENANT_USER);
        System.out.println("\n\n\n\n\n");

        automationContext1 = getAutomationContextWithKey("mb002");
        automationContext2 = getAutomationContextWithKey("mb003");

        andesAdminClient1 = new AndesAdminClient(automationContext1.getContextUrls().getBackEndUrl(),
                super.login(automationContext1), ConfigurationContextProvider.getInstance().getConfigurationContext());

        andesAdminClient2 = new AndesAdminClient(automationContext2.getContextUrls().getBackEndUrl(),
                super.login(automationContext2), ConfigurationContextProvider.getInstance().getConfigurationContext());

    }

    @Test(groups = "wso2.mb", description = "Single queue send-receive test case", enabled = true)
    public void performSingleQueueSendReceive() throws XPathExpressionException,
            AndesAdminServiceBrokerManagerAdminException, RemoteException {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        String hostinfo1 = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("qpid");

        AndesClient receivingClient = new AndesClient("receive", hostinfo1
                , "queue:singleQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        boolean bQueueReplicated = false;
        Queue[] queues = andesAdminClient2.getAllQueues();

        for (Queue queue : queues) {
            if (queue.getQueueName().equalsIgnoreCase("singleQueue")) {
                bQueueReplicated = true;
                break;
            }
        }

        assertTrue(bQueueReplicated, "Queue created in MB node 1 not replicated in MB node 2");

        String hostinfo2 = automationContext2.getInstance().getHosts().get("default") + ":" +
                automationContext2.getInstance().getPorts().get("qpid");

        AndesClient sendingClient = new AndesClient("send", hostinfo2
                , "queue:singleQueue", "100", "false",
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


    @Test(groups = "wso2.mb", description = "Single queue send-receive test case", enabled = true)
    public void testQueueMessages() throws Exception {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        String hostinfo1 = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("qpid");

        andesAdminClient1.createQueue("singleQueue2");

        /*AndesClient receivingClient = new AndesClient("receive", hostinfo1
                , "queue:singleQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();*/

        boolean bQueueReplicated = false;
        Queue queue = andesAdminClient2.getQueueByName("singleQueue2");

        assertTrue(queue != null && queue.getQueueName().equalsIgnoreCase("singleQueue2") ,
                "Queue created in MB node 1 not replicated in MB node 2");

        String hostinfo2 = automationContext2.getInstance().getHosts().get("default") + ":" +
                automationContext2.getInstance().getPorts().get("qpid");

        AndesClient sendingClient = new AndesClient("send", hostinfo2
                , "queue:singleQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();
        Thread.sleep(20000);

        queue = andesAdminClient2.getQueueByName("singleQueue2");
        assertTrue(queue.getMessageCount() == expectedCount, "Messages not received at MB node");

        queue = andesAdminClient1.getQueueByName("singleQueue2");
        assertTrue(queue.getMessageCount() == expectedCount, "Messages not replicated at MB node");

    }


    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {
    }

}