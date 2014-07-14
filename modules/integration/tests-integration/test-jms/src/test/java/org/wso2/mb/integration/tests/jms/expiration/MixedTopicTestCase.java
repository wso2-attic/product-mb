package org.wso2.mb.integration.tests.jms.expiration;

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

/**
 * Created with IntelliJ IDEA.
 * User: hasithad
 * Date: 7/11/14
 * Time: 1:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class MixedTopicTestCase extends MBIntegrationBaseTest {

    private static final Log log = LogFactory.getLog(MixedTopicTestCase.class);

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = "wso2.mb", description = "Single topic send-receive test case")
    public void performSingleTopicSendReceiveTestCase() {

        Integer sendNormalCount = 600;
        Integer sendExpiredcount = 400;
        Integer runTime = 20;
        Integer expectedCount = 600;
        String expiration = "100";

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:5672", "topic:singleTopic",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, "");

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "topic:singleTopic", "100", "false",
                runTime.toString(), sendNormalCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendNormalCount, "");

        sendingClient.startWorking();

        AndesClient sendingExpiredClient = new AndesClient("send", "127.0.0.1:5672", "topic:singleTopic", "100", "false",
                runTime.toString(), sendExpiredcount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendExpiredcount, "",expiration);

        sendingExpiredClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,expectedCount);

        if(receiveSuccess && sendSuccess) {
            log.info("TEST PASSED");
        }  else {
            log.info("TEST FAILED");
        }
        assertEquals((receiveSuccess && sendSuccess), true);
    }

}
