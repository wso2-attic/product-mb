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

package org.wso2.mb.integration.common.utils.ui.pages.main;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.wso2.mb.integration.common.utils.ui.pages.MBPage;
import org.wso2.mb.integration.common.utils.ui.UIElementMapper;

import java.io.IOException;

/**
 * Home page class holds the information of product page you got once login
 * NOTE: To navigate to a page Don't use direct links to pages. To ensure there is a UI element to navigate to
 * that page.
 */
public class HomePage extends MBPage {

    /**
     * Checks whether the current page is the home page. if not throws a runtime exception
     * @param driver WebDriver
     */
    public HomePage(WebDriver driver) {
        super(driver);
        // Check that we're on the right page.
        if (!driver.findElement(By.id(UIElementMapper.getInstance()
                .getElement("home.dashboard.middle.text"))).getText().contains("Home")) {
            throw new IllegalStateException("This is not the home page");
        }
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
}
