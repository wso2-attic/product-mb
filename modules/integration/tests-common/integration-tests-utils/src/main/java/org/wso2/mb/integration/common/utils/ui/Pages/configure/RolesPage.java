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
package org.wso2.mb.integration.common.utils.ui.Pages.configure;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.wso2.mb.integration.common.utils.ui.UIElementMapper;

import java.util.List;

public class RolesPage {

    private static final Log log = LogFactory.getLog(RolesPage.class);
    private WebDriver driver;

    public RolesPage(WebDriver driver){
        this.driver = driver;

        if (!driver.findElement(By.xpath(UIElementMapper.getInstance().getElement("configure.usr.mgt.roles.header.xpath"))).getText().contains("Roles")) {
            throw new IllegalStateException("This is not the Roles page");
        }
    }

    boolean addNewRole(final String roleName, final List<String> permissionList){
        boolean isSuccessful = false;



        return isSuccessful;
    }

    public AddRoleStep1Page getAddRolePage() {
        driver.findElement(By.xpath(UIElementMapper.getInstance().getElement("usr.mgt.roles.add.new.role.button.xpath"))).click();
        return new AddRoleStep1Page(driver);
    }
}
