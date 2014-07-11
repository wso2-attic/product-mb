package org.wso2.mb.integration.tests.server.mgt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.annotations.ExecutionEnvironment;
import org.wso2.carbon.automation.engine.annotations.SetEnvironment;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.integration.common.extensions.carbonserver.MultipleServersManager;

import java.util.HashMap;
import java.util.Map;

public class NewInstanceTestCase {
    private MultipleServersManager manager = new MultipleServersManager();
    private Map<String, String> startupParameterMap1 = new HashMap<String, String>();
    private Map<String, String> startupParameterMap2 = new HashMap<String, String>();
    private AutomationContext context;
    private static final Log log = LogFactory.getLog(NewInstanceTestCase.class);



    @SetEnvironment(executionEnvironments = {ExecutionEnvironment.STANDALONE})
    @BeforeClass(groups = {"esb.multi.server"})
    public void testStartServers() throws Exception {
        context = new AutomationContext();
        startupParameterMap1.put("-DportOffset", "2");
        CarbonTestServerManager server1 = new CarbonTestServerManager(context, System.getProperty("carbon.zip"),
                startupParameterMap1);

        manager.startServers(server1);
    }

    @SetEnvironment(executionEnvironments = {ExecutionEnvironment.STANDALONE})
    @Test(groups = {"esb.multi.server"})
    public void test() {
        log.info("Test server startup with system properties");
    }

    @SetEnvironment(executionEnvironments = {ExecutionEnvironment.STANDALONE})
    @AfterClass
    public void clean() throws Exception {
        manager.stopAllServers();
    }
}
