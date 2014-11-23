/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.mb.integration.tests.mqtt.load;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.operations.mqtt.MQTTClientEngine;
import org.wso2.mb.integration.common.clients.operations.mqtt.MQTTConstants;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.AndesMQTTClient;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.ClientMode;
import org.wso2.mb.integration.common.clients.operations.mqtt.QualityOfService;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

/**
 * Send and receive via multiple publisher and multiple subscribers.
 */
public class MultiThreadedMQTTTestCase extends MBIntegrationBaseTest {

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
     * Send a large amount of messages and receive via multiple MQTT clients.
     *
     * @throws MqttException
     */
    @Test(groups = {"wso2.mb", "mqtt"}, description = "Send a large amount of messages and receive via multiple MQTT " +
            "clients")
    public void performMultiThreadedMQTTTestCase() throws MqttException {
        String topicName = "MultiThreadedTopic";
        int sendCount = 100000;
        int noOfPublishers = 10;
        int noOfSubscribers = 10;

        MQTTClientEngine mqttClientEngine = new MQTTClientEngine();

        //create the subscribers
        mqttClientEngine.createSubscriberConnection(topicName, QualityOfService.MOST_ONCE, noOfSubscribers, false,
                ClientMode.BLOCKING);

        mqttClientEngine.createPublisherConnection(topicName, QualityOfService.MOST_ONCE,
                MQTTConstants.TEMPLATE_PAYLOAD, noOfPublishers, sendCount / noOfPublishers, ClientMode.BLOCKING);

        mqttClientEngine.waitUntilAllMessageReceivedAndShutdownClients();

        Assert.assertEquals(mqttClientEngine.getSentMessageCount(), sendCount, "Published message count is incorrect.");


        for (AndesMQTTClient subscriberClient : mqttClientEngine.getSubscriberList()) {
            Assert.assertEquals(subscriberClient.getReceivedMessageCount(), sendCount,
                    "The received message count is incorrect for client " + subscriberClient.getMqttClientID());
        }
    }
}
