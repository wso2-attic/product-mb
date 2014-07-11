/*
*Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*WSO2 Inc. licenses this file to you under the Apache License,
*Version 2.0 (the "License"); you may not use this file except
*in compliance with the License.
*You may obtain a copy of the License at
*
*http://www.apache.org/licenses/LICENSE-2.0
*
*Unless required by applicable law or agreed to in writing,
*software distributed under the License is distributed on an
*"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*KIND, either express or implied.  See the License for the
*specific language governing permissions and limitations
*under the License.
*/

package org.wso2.mb.integration.common.utils.ui.Pages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.wso2.mb.integration.common.utils.ui.UIElementMapper;

import java.io.IOException;

/**
 * Home page class holds the information of product page you got once login
 * NOTE: To navigate to a page Don't use direct links to pages. To ensure there is a UI element to navigate to
 * that page.
 */
public class HomePage {

    private static final Log log = LogFactory.getLog(HomePage.class);
    private WebDriver driver;

    public HomePage(WebDriver driver) throws IOException {
        this.driver = driver;
        // Check that we're on the right page.
        if (!driver.findElement(By.id(UIElementMapper.getInstance()
                .getElement("home.dashboard.middle.text"))).getText().contains("Home")) {
            throw new IllegalStateException("This is not the home page");
        }
    }

    public LoginPage logout() throws IOException {
        driver.findElement(By.xpath(UIElementMapper.getInstance().getElement("home.mb.sign.out.xpath"))).click();
        return new LoginPage(driver);
    }

    public DLCBrowsePage getDLCBrowsePage() throws Exception {
        driver.findElement(By.xpath(UIElementMapper.getInstance().getElement("home.mb.dlc.browse.xpath"))).click();
        return new DLCBrowsePage(driver);
    }

    public QueuesBrowsePage getQueuesBrowsePage() throws IOException {
        driver.findElement(By.xpath(UIElementMapper.getInstance().getElement("home.mb.queues.browse.xpath"))).click();
        return new QueuesBrowsePage(driver);
    }

    public QueueAddPage getQueueAddPage() throws IOException {
        driver.findElement(By.xpath(UIElementMapper.getInstance().getElement("home.mb.queues.add.xpath"))).click();
        return new QueueAddPage(driver);
    }

    public ConfigurePage getConfigurePage() throws Exception {
        driver.findElement(By.id(UIElementMapper.getInstance().getElement("configure.tab.id"))).click();
        return new ConfigurePage(driver);
    }
}
