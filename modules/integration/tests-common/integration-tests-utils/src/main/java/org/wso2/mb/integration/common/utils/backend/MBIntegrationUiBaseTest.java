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
 * under the License.
 */

package org.wso2.mb.integration.common.utils.backend;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.engine.frameworkutils.FrameworkPathUtil;
import org.wso2.carbon.automation.extensions.selenium.BrowserManager;
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.mb.integration.common.utils.ui.UIElementMapper;
import org.wso2.mb.integration.common.utils.ui.pages.login.LoginPage;

import javax.xml.xpath.XPathExpressionException;
import java.io.File;

public class MBIntegrationUiBaseTest {
    private static final Log log = LogFactory.getLog(MBIntegrationUiBaseTest.class);
    protected AutomationContext mbServer;
    protected String sessionCookie;
    protected String backendURL;
    protected ServerConfigurationManager serverManager;
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

    /**
     * Get current test user's Username according to the automation context
     *
     * @throws XPathExpressionException
     */
    protected String getCurrentUserName() throws XPathExpressionException {
        return mbServer.getContextTenant().getContextUser().getUserName();
    }

    /**
     * Get current test user's password according to the automation context
     *
     * @throws XPathExpressionException
     */
    protected String getCurrentPassword() throws XPathExpressionException {
        return mbServer.getContextTenant().getContextUser().getPassword();
    }


    /**
     * Restart the testing MB server with WSO2 domain name set under user management
     *
     * @throws Exception
     */
    protected void restartServerWithDomainName() throws Exception {
        serverManager = new ServerConfigurationManager(mbServer);

        // Replace the user-mgt.xml with the new configuration and restarts the server.
        serverManager.applyConfiguration(new File(FrameworkPathUtil.getSystemResourceLocation() + File.separator +
                "artifacts" + File.separator + "mb" + File.separator + "config" + File.separator
                + "user-mgt.xml"), new File(ServerConfigurationManager.getCarbonHome() +
                File.separator + "repository" + File.separator + "conf" + File.separator +
                "user-mgt.xml"), true, true);
    }

    protected void restartInPreviousConfiguration() throws Exception {
        serverManager.restoreToLastConfiguration(true);
    }

    protected String getLoginURL() throws Exception{
        return "https://localhost:9443/carbon/";
    }

    protected LoginPage logout() throws Exception {
        driver.findElement(By.xpath(UIElementMapper.getInstance().getElement("home.mb.sign.out.xpath"))).click();
        return new LoginPage(driver);
    }
}
