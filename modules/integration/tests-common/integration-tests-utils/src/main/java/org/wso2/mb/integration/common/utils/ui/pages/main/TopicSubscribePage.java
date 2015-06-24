/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*/

package org.wso2.mb.integration.common.utils.ui.pages.main;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.Select;
import org.wso2.mb.integration.common.utils.ui.UIElementMapper;
import org.wso2.mb.integration.common.utils.ui.pages.MBPage;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The class for adding a subscription for topic. Provides functions available in the subscription adding page.
 */
public class TopicSubscribePage extends MBPage{

    /**
     * Constructor. Takes the reference of web driver instance.
     *
     * @param driver The selenium Web Driver
     */
    protected TopicSubscribePage(WebDriver driver) {
        super(driver);

        // Check that we're on the right page.
        if (!driver.findElement(By.xpath(UIElementMapper.getInstance()
                                .getElement("mb.topic.subscribe.page.header.xpath"))).getText().contains("Subscribe")) {
            throw new IllegalStateException("This is not the Topic Subscribe page");
        }
    }

    /**
     * Creates a new topic subscription.
     *
     * @param subscriptionMode The subscription mode. Can be "Topic Only", "Immediate Children" or "Topic and Children".
     * @param eventSinkUrl The URL at which the subscription is being listened.
     * @param expirationDate The expiration date of the subscription.
     * @return True if subscription is successfully added. Else false.
     */
    public boolean addSubscription(String subscriptionMode, String eventSinkUrl, Date expirationDate){
        boolean isSuccessful = false;

        // Selection subscription mode
        Select dropdown = new Select(driver.findElement(By.id("subscriptionModes")));
        dropdown.selectByVisibleText(subscriptionMode);

        // Setting event sink url
        WebElement eventSinkUrlInput = driver.findElement(By.id("subURL"));
        eventSinkUrlInput.sendKeys(eventSinkUrl);

        // Setting expiration date
        if (null != expirationDate){
            DateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd");
            driver.findElement(By.id("expirationTime")).sendKeys(dateFormatter.format(expirationDate));

            dateFormatter = new SimpleDateFormat("hh");
            driver.findElement(By.id("hhid")).sendKeys(dateFormatter.format(expirationDate));

            dateFormatter = new SimpleDateFormat("mm");
            driver.findElement(By.id("mmid")).sendKeys(dateFormatter.format(expirationDate));

            dateFormatter = new SimpleDateFormat("ss");
            driver.findElement(By.id("ssid")).sendKeys(dateFormatter.format(expirationDate));
        }

        // Clicking "Subscribe" button
        driver.findElement(By.xpath(UIElementMapper.getInstance().
                getElement("mb.topic.subscribe.page.subscribe.button.xpath"))).click();

        String dialog = driver.getWindowHandle();
        driver.switchTo().window(dialog);

        // Checking if valid message is prompt on the dialog
        if (driver.findElement(By.id(UIElementMapper.getInstance().getElement("mb.popup.dialog.id"))).getText()
                                                                            .toLowerCase().contains("successfully")) {
            isSuccessful = true;

            // Clicking ok button of the dialog
            driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.topic.subscribe.page.subscribe.okbutton.xpath"))).click();
        }

        return isSuccessful;
    }
}
