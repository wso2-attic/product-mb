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
import org.wso2.carbon.integration.common.extensions.carbonserver.MultipleServersManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;


public class NewInstanceTestCase {
    private MultipleServersManager manager = new MultipleServersManager();
    private Map<String, String> startupParameterMap1 = new HashMap<String, String>();
    private AutomationContext context;
    private long TIMEOUT=180000;
    private static final Log log = LogFactory.getLog(NewInstanceTestCase.class);



    @SetEnvironment(executionEnvironments = {ExecutionEnvironment.STANDALONE})
    @BeforeClass(groups =  {"mb.server.startup"})
    public void testStartServers() throws Exception {
        context = new AutomationContext();
        startupParameterMap1.put("-DportOffset", "2");
        CarbonTestServerManager server1 = new CarbonTestServerManager(context, System.getProperty("carbon.zip"),
                startupParameterMap1);
        manager.startServers(server1);
    }

    @SetEnvironment(executionEnvironments = {ExecutionEnvironment.STANDALONE})
    @Test(groups = {"mb.server.startup"})
    public void waitForPortTestCase() {
        boolean isPortOpen = false;
        long startTime = System.currentTimeMillis();
        String hostName = "localhost";

        while (!isPortOpen && (System.currentTimeMillis() - startTime) < TIMEOUT) {
            Socket socket = null;
            try {
                InetAddress address = InetAddress.getByName(hostName);
                socket = new Socket(address, 9445);
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
                    log.error("Can not close the socket which is used to check the server status ",e);
                }
            }
        }
        Assert.assertTrue(isPortOpen);
    }


    @SetEnvironment(executionEnvironments = {ExecutionEnvironment.STANDALONE})
    @Test(groups = {"mb.server.startup"})
    public    void waitForLoginTestCase()  {
        long startTime = System.currentTimeMillis();
        boolean loginFailed = true;
        while (((System.currentTimeMillis() - startTime) < TIMEOUT) && loginFailed) {
            log.info("Waiting to login user...");
            try {
                LoginLogoutClient loginClient = new LoginLogoutClient("https://localhost:9445/services/","admin","admin");
                loginClient.login();
                loginFailed = false;
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.info("Login failed after server startup", e);
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {
                    // Nothing to do
                }
            }
        }

        Assert.assertFalse(loginFailed);

    }


    @SetEnvironment(executionEnvironments = {ExecutionEnvironment.STANDALONE})
    @AfterClass
    public void clean() throws Exception {
        manager.stopAllServers();
    }
}
