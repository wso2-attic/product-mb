/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import org.eclipse.paho.client.mqttv3.MqttException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.blocking.MQTTBlockingSubscriberClient;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.MQTTClientConnectionConfiguration;
import org.wso2.mb.integration.common.clients.operations.mqtt.MQTTConstants;
import org.wso2.mb.integration.common.clients.operations.mqtt.QualityOfService;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DuplicateClientIdTestCase extends MBIntegrationBaseTest {

    private static final ExecutorService clientControlSubscriptionThreads = Executors.newFixedThreadPool(2);

    /**
     * Initialize super class.
     * @throws Exception
     */
    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
    }

    /**
     * Subscribe two clients with same clientId.
     * When second client is connecting first client should disconnect.
     */
    @Test(groups = {"wso2.mb", "mqtt"})
    public void performDuplicateClientIdTestCase() throws MqttException {

        String clientId = "duplicateClientId";
        String topicName = "duplicateClientIdTopic";
        QualityOfService qos = QualityOfService.MOST_ONCE;
        MQTTClientConnectionConfiguration configuration = new MQTTClientConnectionConfiguration();

        configuration.setBrokerHost(MQTTConstants.BROKER_HOST);
        configuration.setBrokerProtocol(MQTTConstants.BROKER_PROTOCOL);
        configuration.setBrokerPort(MQTTConstants.BROKER_PORT);
        configuration.setBrokerPassword(MQTTConstants.BROKER_PASSWORD);
        configuration.setBrokerUserName(MQTTConstants.BROKER_USER_NAME);
        configuration.setCleanSession(true);

        // Create mqtt client
        MQTTBlockingSubscriberClient mqttClient = new MQTTBlockingSubscriberClient(configuration, clientId, topicName, qos, false);
        clientControlSubscriptionThreads.execute(mqttClient);

        try {
            Thread.sleep(1000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Create a duplicate mqtt client with the same clientId
        MQTTBlockingSubscriberClient mqttDuplicateClient = new MQTTBlockingSubscriberClient(configuration, clientId, topicName, qos, false);
        clientControlSubscriptionThreads.execute(mqttDuplicateClient);

        try {
            Thread.sleep(1000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // at this point, mqttDuplicateClient should be connected and mqttClient should not
        Assert.assertTrue(mqttDuplicateClient.isConnected(), "Duplicate client is not connected.");
        Assert.assertFalse(mqttClient.isConnected(), "Original client is still connected.");

    }
}
