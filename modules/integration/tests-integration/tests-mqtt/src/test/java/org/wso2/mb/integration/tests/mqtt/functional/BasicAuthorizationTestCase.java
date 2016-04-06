/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.mb.integration.tests.mqtt.functional;

import org.apache.commons.configuration.ConfigurationException;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.integration.common.admin.client.LogViewerClient;
import org.wso2.carbon.integration.common.admin.client.UserManagementClient;
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;
import org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.carbon.logging.view.stub.LogViewerLogViewerException;
import org.wso2.carbon.logging.view.stub.types.carbon.LogEvent;
import org.wso2.carbon.registry.resource.stub.ResourceAdminServiceExceptionException;
import org.wso2.carbon.um.ws.api.stub.UserStoreExceptionException;
import org.wso2.carbon.user.mgt.stub.UserAdminUserAdminException;
import org.wso2.mb.integration.common.clients.ClientMode;
import org.wso2.mb.integration.common.clients.MQTTClientConnectionConfiguration;
import org.wso2.mb.integration.common.clients.MQTTClientEngine;
import org.wso2.mb.integration.common.clients.MQTTConstants;
import org.wso2.mb.integration.common.clients.QualityOfService;
import org.wso2.mb.integration.common.utils.backend.ConfigurationEditor;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;
import org.wso2.mb.integration.tests.mqtt.functional.util.RemoteAuthorizationManagerServiceClient;
import org.wso2.mb.integration.tests.mqtt.functional.util.ResourceAdminServiceClient;

import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Verifies basic mqtt message transactions are functional.
 * <p/>
 * Send a single mqtt messages with unauthorized user and not recieve.
 * Send messages with authorized user and receive them.
 */
public class BasicAuthorizationTestCase extends MBIntegrationBaseTest {

    private UserManagementClient userMgtClient;
    private ResourceAdminServiceClient resourceAdminServiceClient;
    private static final String TOPIC = "authorization/test";
    private LogViewerClient logViewerClient;
    /**
     * Initialize super class.
     *
     * @throws Exception
     */
    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
    }

    /**
     * Setup test configuration by creating users and providing permission to access topics.
     */
    @BeforeClass
    public void setupConfiguration() throws XPathExpressionException, IOException, ConfigurationException,
                                            AutomationUtilException, UserAdminUserAdminException,
                                            UserStoreExceptionException, ResourceAdminServiceExceptionException {

        super.serverManager = new ServerConfigurationManager(automationContext);
        String defaultMBConfigurationPath = ServerConfigurationManager.getCarbonHome() +
                File.separator + "repository" + File.separator + "conf" + File.separator + "broker.xml";

        ConfigurationEditor configurationEditor = new ConfigurationEditor(defaultMBConfigurationPath);

        configurationEditor.updateProperty(AndesConfiguration.TRANSPORTS_MQTT_USER_AUTHENTICATION, "REQUIRED");
        configurationEditor.updateProperty(AndesConfiguration.TRANSPORTS_MQTT_USER_AUTHORIZATION, "REQUIRED");
        configurationEditor.applyUpdatedConfigurationAndRestartServer(serverManager);

        LoginLogoutClient loginLogoutClient = new LoginLogoutClient(automationContext);
        String sessionCookie = loginLogoutClient.login();
        userMgtClient = new UserManagementClient(backendURL, sessionCookie);
        userMgtClient.addUser("user1-mqtt", "passWord1@", null, "default");
        userMgtClient.addUser("user2-mqtt", "passWord1@", null, "default");

        resourceAdminServiceClient = new ResourceAdminServiceClient(backendURL, sessionCookie);
        resourceAdminServiceClient.addCollection("/_system/governance/permission/admin/mqtt","connect", "", "");
        resourceAdminServiceClient.addCollection("/_system/governance/permission/admin/mqtt/topic/authorization" , "test", "", "");

        RemoteAuthorizationManagerServiceClient  remoteAuthorizationManagerServiceClient = new RemoteAuthorizationManagerServiceClient(backendURL, sessionCookie);
        remoteAuthorizationManagerServiceClient.authorizeRole("mqtt-connect", "/permission/admin/mqtt/connect", "authorize");
        remoteAuthorizationManagerServiceClient.authorizeRole("mqtt-publish", "/permission/admin/mqtt/topic/authorization/test", "publish");
        remoteAuthorizationManagerServiceClient.authorizeRole("mqtt-subscribe", "/permission/admin/mqtt/topic/authorization/test", "subscribe");

        String users[] = new String[1];
        users[0] = "user2-mqtt";
        // Create roles
        userMgtClient.addRole("mqtt-connect", users, null);
        userMgtClient.addRole("mqtt-publish", users, null);
        userMgtClient.addRole("mqtt-subscribe", users, null);

        logViewerClient = new LogViewerClient(backendURL, sessionCookie);
    }

    /**
     * Send a single mqtt message on qos {@link QualityOfService#LEAST_ONCE} and receive.
     *
     * @throws MqttException
     */
    @Test(groups = {"wso2.mb", "mqtt"}, description = "Single mqtt message send receive test case")
    public void performConnectionAuthorizationTestWithUnAuthorizedUser()
            throws MqttException, XPathExpressionException, LogViewerLogViewerException, RemoteException {
        try {
            int noOfMessages = 1;
            MQTTClientEngine mqttClientEngine = new MQTTClientEngine();
            MQTTClientConnectionConfiguration mqttClientConnectionConfiguration = mqttClientEngine.getConfigurations(
                    automationContext);
            mqttClientConnectionConfiguration.setBrokerUserName("user1-mqtt");
            mqttClientConnectionConfiguration.setBrokerPassword("passWord1@");
            mqttClientEngine.createPublisherConnection(mqttClientConnectionConfiguration, TOPIC,
                                                       QualityOfService.LEAST_ONCE,
                                                       MQTTConstants.TEMPLATE_PAYLOAD, noOfMessages,
                                                       ClientMode.BLOCKING);
            LogEvent[] logs = logViewerClient.getAllRemoteSystemLogs();
            boolean isPublished = false;
            for (LogEvent logEvent : logs) {
                String message = logEvent.getMessage();
                if (message.contains("Lost connection with client")) {
                    isPublished = true;
                }
            }
            logViewerClient.clearLogs();
            Assert.assertTrue(isPublished, "Access is been granted for unauthorized user");
        } catch (MqttException e) {
            Assert.assertTrue(true);
        }

    }

    @Test(groups = {"wso2.mb", "mqtt"}, description = "Single mqtt message send receive test case")
    public void performAuthorizationTestWithAuthorizedUser() throws MqttException, XPathExpressionException {
        int noOfMessages = 1;
        boolean saveMessages = true;
        MQTTClientEngine mqttClientEngine = new MQTTClientEngine();
        MQTTClientConnectionConfiguration mqttClientConnectionConfiguration = mqttClientEngine.getConfigurations(automationContext);
        mqttClientConnectionConfiguration.setBrokerUserName("user2-mqtt");
        mqttClientConnectionConfiguration.setBrokerPassword("passWord1@");
        mqttClientEngine.createSubscriberConnection(mqttClientConnectionConfiguration, TOPIC,
                                                    QualityOfService.LEAST_ONCE, saveMessages,
                                                    ClientMode.BLOCKING);
        mqttClientEngine.createPublisherConnection(mqttClientConnectionConfiguration, TOPIC,
                                                   QualityOfService.LEAST_ONCE,
                                                   MQTTConstants.TEMPLATE_PAYLOAD, noOfMessages,
                                                   ClientMode.BLOCKING);
        mqttClientEngine.waitUntilAllMessageReceivedAndShutdownClients();
        List<MqttMessage> receivedMessages = mqttClientEngine.getReceivedMessages();
        Assert.assertEquals(receivedMessages.size(), noOfMessages, "The received message count is incorrect.");
        Assert.assertEquals(receivedMessages.get(0).getPayload(), MQTTConstants.TEMPLATE_PAYLOAD,
                            "The received message is incorrect");
    }

    @Test(groups = {"wso2.mb", "mqtt"}, description = "Single mqtt message send receive test case")
    public void performAuthorizationTestWithUnAuthorizedUser()
            throws MqttException, XPathExpressionException, LogViewerLogViewerException, RemoteException {
        int noOfMessages = 1;
        MQTTClientEngine mqttClientEngine = new MQTTClientEngine();
        MQTTClientConnectionConfiguration mqttClientConnectionConfiguration = mqttClientEngine.getConfigurations(automationContext);
        mqttClientConnectionConfiguration.setBrokerUserName("user2-mqtt");
        mqttClientConnectionConfiguration.setBrokerPassword("passWord1@");
        mqttClientEngine.createPublisherConnection(mqttClientConnectionConfiguration, "authorization",
                                                   QualityOfService.LEAST_ONCE,
                                                   MQTTConstants.TEMPLATE_PAYLOAD, noOfMessages,
                                                   ClientMode.BLOCKING);
        LogEvent[] logs = logViewerClient.getAllRemoteSystemLogs();
        boolean isPublished = false;
        for (LogEvent logEvent : logs) {
            String message = logEvent.getMessage();
            if (message.contains("does not have permission to publish to topic : authorization")) {
                isPublished = true;
            }
        }
        logViewerClient.clearLogs();
        Assert.assertTrue(isPublished, "Access is been granted for unauthorized user");
    }

    /**
     * Restore to the previous configurations when the message content test is complete.
     *
     * @throws IOException
     * @throws AutomationUtilException
     */
    @AfterClass
    public void tearDown() throws IOException, AutomationUtilException {
        super.serverManager.restoreToLastConfiguration(true);
    }
}
