package org.wso2.mb.integration.tests.server.mgt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.annotations.ExecutionEnvironment;
import org.wso2.carbon.automation.engine.annotations.SetEnvironment;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.engine.frameworkutils.FrameworkPathUtil;
import org.wso2.carbon.integration.common.admin.client.AuthenticatorClient;
import org.wso2.carbon.integration.common.extensions.carbonserver.MultipleServersManager;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;


import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.io.File;


public class ProfileTestCase extends MBIntegrationBaseTest {
    private MultipleServersManager manager = new MultipleServersManager();
    private Map<String, String> startupParameterMap1 = new HashMap<String, String>();
    private Map<String, String> startupParameterMap2 = new HashMap<String, String>();
    private AutomationContext context;
    private ServerConfigurationManager serverManager1 = null;
    private ServerConfigurationManager serverManager2 = null;
    private long TIMEOUT = 180000;


    private static final Log log = LogFactory.getLog(NewInstanceTestCase.class);


    @SetEnvironment(executionEnvironments = {ExecutionEnvironment.STANDALONE})
    @BeforeClass(groups = {"mb.server.profiles"})
    public void testStartServers() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        context = new AutomationContext();
        startupParameterMap1.put("-Dprofile", "cassandra");
        startupParameterMap1.put("-DportOffset", "1");
        CarbonTestServerManager server1 = new CarbonTestServerManager(context, System.getProperty("carbon.zip"),
                startupParameterMap1);

        manager.startServers(server1);
        serverManager1 = new ServerConfigurationManager(automationContext);
        serverManager2 = new ServerConfigurationManager(automationContext);

        //changing the andes-virtualhosts.xml with the new configuration and restarts the server
        serverManager1.applyConfiguration(new File(FrameworkPathUtil.getSystemResourceLocation() + File.separator
                + "artifacts" + File.separator + "mb" + File.separator + "config" + File.separator + "advanced"
                + File.separator + "andes-virtualhosts.xml"),
                new File(ServerConfigurationManager.getCarbonHome() + File.separator + "repository" + File.separator
                        + "conf" + File.separator + "advanced" + File.separator + "andes-virtualhosts.xml"), true, false);
        //changing andes-config.xml
        serverManager2.applyConfiguration(new File(FrameworkPathUtil.getSystemResourceLocation() + File.separator
                + "artifacts" + File.separator + "mb" + File.separator + "config" + File.separator + "advanced"
                + File.separator + "andes-config.xml"),
                new File(ServerConfigurationManager.getCarbonHome() + File.separator + "repository" + File.separator
                        + "conf" + File.separator + "advanced" + File.separator + "andes-config.xml"), true, true);
    }


    @Test(groups = "mb.server.profiles", description = "Change the andes-virtualhosts and andes-config to rename the existing virtual host and start MB")
    public void testCassandraProfile() throws Exception {
        long startTime = System.currentTimeMillis();
        boolean loginFailed = true;
        boolean isPortOpen = false;
        String hostName = "localhost";

        while (!isPortOpen && (System.currentTimeMillis() - startTime) < TIMEOUT) {
            Socket socket = null;
            try {
                InetAddress address = InetAddress.getByName(hostName);
                socket = new Socket(address, 9443);
                isPortOpen = socket.isConnected();
            } catch (IOException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            } finally {
                try {
                    if ((socket != null) && (socket.isConnected())) {
                        socket.close();
                    }
                } catch (IOException e) {
                    log.error("Can not close the socket which is used to check the server status ", e);
                }
            }
        }

        Assert.assertTrue(isPortOpen);
        while (((System.currentTimeMillis() - startTime) < TIMEOUT) && loginFailed) {
            log.info("Waiting to login user...");
            try {
                AuthenticatorClient authenticatorClient =
                        new AuthenticatorClient(automationContext.getContextUrls().getBackEndUrl());
                authenticatorClient.login(automationContext.getSuperTenant().getContextUser().getUserName(),
                        automationContext.getSuperTenant().getContextUser().getPassword(), "127.0.0.1");
                loginFailed = false;
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Login failed after server startup", e);
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {
                    // Nothing to do
                }
            }
        }

        Assert.assertFalse(loginFailed);
        log.info("Successfully logged into MB.");

    }


    @SetEnvironment(executionEnvironments = {ExecutionEnvironment.STANDALONE})
    @AfterClass(groups = {"mb.server.profiles"})
    public void clean() throws Exception {
        // Restoring the andes-virtualhosts config and andes-config.xml
        serverManager2.restoreToLastConfiguration(false);
        serverManager1.restoreToLastConfiguration();
        manager.stopAllServers();
    }

}
