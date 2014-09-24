/*
 *
 *   Copyright (c) 2005-2011, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.carbon.mb.ui.test.configure;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationUiBaseTest;
import org.wso2.mb.integration.common.utils.ui.pages.configure.*;
import org.wso2.mb.integration.common.utils.ui.pages.login.LoginPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.HomePage;

/**
 * Creates a new user with login permission using the super user admin account.
 * Then logs into the created new user account with new users user credentials
 */
public class NewUserPermissionTest extends MBIntegrationUiBaseTest{

    @BeforeClass()
    public void init() throws Exception{
        super.init();
    }

    @Test()
    public void createNewUser() throws Exception {
        // login to admin account
        driver.get(getLoginURL());
        LoginPage loginPage = new LoginPage(driver);
        HomePage homePage = loginPage.loginAs(mbServer.getContextTenant().getContextUser().getUserName(),
                mbServer.getContextTenant().getContextUser().getPassword());

        //create a new login user role with login permission
        ConfigurePage configurePage = homePage.getConfigurePage();
        UserManagementPage usrMgtPage = configurePage.getUserManagementPage();
        RolesPage rolesPage = usrMgtPage.getRolesPage();
        AddRoleStep1Page step1 = rolesPage.getAddRolePage();
        step1.setDetails("loginRole");
        AddRoleStep2Page step2 = step1.next();
        step2.selectPermission("usr.mgt.add.role.step2.login.role.xpath");
        AddRoleStep3Page step3 = step2.next();

        // assert whether the role was successfully created
        Assert.assertEquals(step3.finish(), true);

        // create a new user and assign newly created login user role
        configurePage = new ConfigurePage(driver);
        usrMgtPage = configurePage.getUserManagementPage();
        UsersPage usersPage = usrMgtPage.getUsersPage();
        AddUserStep1Page addUserStep1Page = usersPage.getAddNewUserPage();
        addUserStep1Page.addUserDetails("loginUser", "password", "password");
        AddUserStep2Page addUserStep2Page = addUserStep1Page.next();
        addUserStep2Page.selectRole("loginRole");
        addUserStep2Page.finish();
        loginPage = logout();

        // login with new user account
        homePage = loginPage.loginAs("loginUser", "password");
        homePage.logout();
    }

    @AfterClass()
    public void tearDown() {
        driver.quit();
    }

}
