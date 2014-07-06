package org.wso2.mb.integration.common.utils.backend;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openqa.selenium.WebDriver;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.extensions.selenium.BrowserManager;
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;

public class MBIntegrationUiBaseTest {
    private static final Log log = LogFactory.getLog(MBIntegrationUiBaseTest.class);
    protected AutomationContext mbServer;
    protected String sessionCookie;
    protected String backendURL;
    protected LoginLogoutClient loginLogoutClient;
    protected WebDriver driver;

    protected void init() throws Exception {
        mbServer = new AutomationContext("MB", TestUserMode.SUPER_TENANT_ADMIN);
        loginLogoutClient = new LoginLogoutClient(mbServer);
        sessionCookie = loginLogoutClient.login();
        backendURL = mbServer.getContextUrls().getBackEndUrl();
        this.driver = BrowserManager.getWebDriver();
    }

    protected void init(TestUserMode testUserMode) throws Exception {
        mbServer = new AutomationContext("MB", testUserMode);
        loginLogoutClient = new LoginLogoutClient(mbServer);
        sessionCookie = loginLogoutClient.login();
        backendURL = mbServer.getContextUrls().getBackEndUrl();
        this.driver = BrowserManager.getWebDriver();
    }

    protected String getLoginURL() throws Exception{
        return "https://localhost:9443/carbon/";
    }
}
