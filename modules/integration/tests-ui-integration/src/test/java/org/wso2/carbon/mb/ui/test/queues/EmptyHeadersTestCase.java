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
package org.wso2.carbon.mb.ui.test.queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationUiBaseTest;
import org.wso2.mb.integration.common.utils.ui.pages.login.LoginPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.HomePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueuesBrowsePage;

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.TimeoutException;

/**
 * The following test case uses rabbitmq amqp client to declare and publish messages to a queue. The message is
 * published without having any headers. This messages should be able to view on the UI.
 */
public class EmptyHeadersTestCase extends MBIntegrationUiBaseTest {

    /**
     * Initializes the test case.
     *
     * @throws AutomationUtilException
     * @throws XPathExpressionException
     * @throws MalformedURLException
     */
    @BeforeClass()
    public void init() throws AutomationUtilException, XPathExpressionException, MalformedURLException {
        super.init();
    }

    /**
     * The test cases uses the rabbitmq amqp client library to publish a message with no headers and view it through the
     * management console.
     * 1. Creates a queue.
     * 2. Publish a message with no headers.
     * 3. Login to management console.
     * 4. Check if messages can be viewed for that queue.
     *
     * @throws IOException
     * @throws TimeoutException
     * @throws XPathExpressionException
     */
    @Test(groups = {"wso2.mb", "queue"})
    public void performEmptyHeadersTestCase() throws IOException, TimeoutException, XPathExpressionException {
        String queueName = "EmptyHeaderQueue";

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setVirtualHost("/carbon");
        factory.setUsername("admin");
        factory.setPassword("admin");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, "amq.direct", queueName);
        String message = "This is a test message";
        channel.basicPublish("amq.direct", queueName, null, message.getBytes());

        channel.close();
        connection.close();

        driver.get(getLoginURL());
        LoginPage loginPage = new LoginPage(driver);
        HomePage homePage = loginPage.loginAs(mbServer.getContextTenant()
                .getContextUser().getUserName(), mbServer.getContextTenant()
                .getContextUser().getPassword());

        QueuesBrowsePage queuesBrowsePage = homePage.getQueuesBrowsePage();
        Assert.assertNotNull(queuesBrowsePage.browseQueue(queueName), "Unable to browse Queue : " + queueName);
    }

    /**
     * Shuts down the selenium web driver
     */
    @AfterClass()
    public void tearDown() {
        driver.quit();
    }
}
