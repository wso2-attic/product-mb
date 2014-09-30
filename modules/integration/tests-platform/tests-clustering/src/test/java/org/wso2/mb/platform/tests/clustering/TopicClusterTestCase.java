package org.wso2.mb.platform.tests.clustering;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.carbon.event.stub.internal.xsd.TopicNode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.topic.TopicAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TopicClusterTestCase extends MBPlatformBaseTest {

    private static final Log log = LogFactory.getLog(TopicClusterTestCase.class);
    private AutomationContext automationContext1;
    private AutomationContext automationContext2;
    private TopicAdminClient topicAdminClient1;
    private TopicAdminClient topicAdminClient2;


    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {

        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);

        automationContext1 = getAutomationContextWithKey("mb002");
        automationContext2 = getAutomationContextWithKey("mb003");

        topicAdminClient1 = new TopicAdminClient(automationContext1.getContextUrls().getBackEndUrl(),
                super.login(automationContext1), ConfigurationContextProvider.getInstance().getConfigurationContext());

        topicAdminClient2 = new TopicAdminClient(automationContext2.getContextUrls().getBackEndUrl(),
                super.login(automationContext2), ConfigurationContextProvider.getInstance().getConfigurationContext());

    }

    @Test(groups = "wso2.mb", description = "Single topic Single node send-receive test case", enabled = false)
    public void testSingleTopicSingleNodeSendReceive() throws Exception {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        String hostinfo = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("qpid");

        AndesClient receivingClient = new AndesClient("receive", hostinfo
                , "topic:singleTopic1",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        boolean bQueueReplicated = false;
        TopicNode topic = topicAdminClient1.getTopicByName("singleTopic1");

        assertTrue(topic.getTopicName().equalsIgnoreCase("singleTopic1"), "Topic created in MB node 1 not exist");

        AndesClient sendingClient = new AndesClient("send", hostinfo
                , "topic:singleTopic1", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        if(receiveSuccess && sendSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }
        assertEquals((receiveSuccess && sendSuccess), true);
    }


    @Test(groups = "wso2.mb", description = "Single topic replication", enabled = false)
    public void testSingleTopicReplication() throws Exception {

        String topic = "singleTopic2";
        boolean bQueueReplicated = false;

        topicAdminClient1.addTopic(topic);
        TopicNode topicNode = topicAdminClient2.getTopicByName(topic);

        assertTrue(topicNode != null && topicNode.getTopicName().equalsIgnoreCase(topic),
                "Topic created in MB node 1 not replicated in MB node 2");

        topicAdminClient2.removeTopic(topic);
        topicNode = topicAdminClient2.getTopicByName(topic);

        assertTrue(topicNode == null,
                "Topic deleted in MB node 2 not deleted in MB node 1");

    }

    @Test(groups = "wso2.mb", description = "Single topic Multi node send-receive test case", enabled = false)
    public void testSingleTopicMultiNodeSendReceive() throws Exception {
        Integer sendCount = 1000;
        Integer runTime = 20;
        Integer expectedCount = 1000;

        String hostinfo1 = automationContext1.getInstance().getHosts().get("default") + ":" +
                automationContext1.getInstance().getPorts().get("qpid");

        AndesClient receivingClient = new AndesClient("receive", hostinfo1
                , "topic:singleTopic3",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        TopicNode topicNode = topicAdminClient2.getTopicByName("singleTopic3");

        String hostinfo2 = automationContext2.getInstance().getHosts().get("default") + ":" +
                automationContext2.getInstance().getPorts().get("qpid");

        AndesClient sendingClient = new AndesClient("send", hostinfo2
                , "topic:singleTopic3", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, "");

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        if(receiveSuccess && sendSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        assertEquals((receiveSuccess && sendSuccess), true);
    }


    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {

        topicAdminClient1.removeTopic("singleTopic1");
        topicAdminClient1.removeTopic("singleTopic2");
        topicAdminClient1.removeTopic("singleTopic3");
    }
}
