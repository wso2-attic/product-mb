/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.carbon.mb.ui.test.configure;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationUiBaseTest;
import org.wso2.mb.integration.common.utils.ui.Pages.*;

public class UserStoreManagementTestCase extends MBIntegrationUiBaseTest {
    @BeforeClass()
    public void init() throws Exception {
        super.init();
    }

    @Test(groups = "wso2.mb", description = "")
    public void paginationTest() throws Exception{
        driver.get(getLoginURL());
        LoginPage loginPage = new LoginPage(driver);
        HomePage homePage = loginPage.loginAs(mbServer.getContextTenant().getContextUser().getUserName(),
                mbServer.getContextTenant().getContextUser().getPassword());
        ConfigurePage configurePage= homePage.getConfigurePage();
        UserStoreManagementPage userStoreManagementPage=  configurePage.getUserStoreManagementPage();
        AddSecondaryUserStorePage addSecondaryUserStorePage = userStoreManagementPage.addSecondaryUserStore();
    }

    @AfterClass()
    public void tearDown() {
        driver.quit();
    }
}
