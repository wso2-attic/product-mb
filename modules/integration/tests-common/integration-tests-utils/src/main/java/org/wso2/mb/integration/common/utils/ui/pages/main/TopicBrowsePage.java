/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import org.wso2.mb.integration.common.utils.ui.UIElementMapper;

import java.io.IOException;

public class TopicBrowsePage {

    private WebDriver driver;

    public TopicBrowsePage(WebDriver driver) throws IOException {
        this.driver = driver;
        // Check that we're on the right page.
        if (!driver.findElement(By.xpath(UIElementMapper.getInstance()
                .getElement("mb.topic.list.page.header.xpath"))).getText().contains("Topic Browse")
                ) {
            throw new IllegalStateException("This is not the Topic Browse page");
        }
    }

    /**
     * Check whether the topic with the given topic name is present in the UI
     * @param topicName topic name
     * @return true if the topic is present, false otherwise
     */
    public boolean isTopicPresent(final String topicName) {
        return driver.getPageSource().contains(topicName);
    }


    /**
     * Delete topic from the UI delete option
     * @param topicName queue name
     * @return true if delete successful, false otherwise
     */
    public boolean deleteTopic(final String topicName) {
        //This method to be implemented
        return true;
    }

}
