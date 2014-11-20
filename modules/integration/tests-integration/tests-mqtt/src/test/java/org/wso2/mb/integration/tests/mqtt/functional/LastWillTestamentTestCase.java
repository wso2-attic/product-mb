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
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.operations.mqtt.MQTTClientEngine;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.ClientMode;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.blocking.MQTTBlockingSubscriberClient;
import org.wso2.mb.integration.common.clients.operations.mqtt.client.MQTTClientConnectionConfiguration;
import org.wso2.mb.integration.common.clients.operations.mqtt.MQTTConstants;
import org.wso2.mb.integration.common.clients.operations.mqtt.QualityOfService;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LastWillTestamentTestCase extends MBIntegrationBaseTest {

    private static final String lastWillTopic = "l/w/t";
    private static final String lastWillMessage = "Last Will Testament";

    private static final ClientMode clientMode = ClientMode.BLOCKING;

    /**
     * Initialize super class.
     * @throws Exception
     */
    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
    }

    @Test(groups = {"wso2.mb", "mqtt"})
    public void performLastWillTestamentTestCase() throws MqttException {
        QualityOfService qualityOfService = QualityOfService.MOST_ONCE;
        MQTTClientEngine mqttClientEngine = new MQTTClientEngine();

        //create the subscribers
        mqttClientEngine.createSubscriberConnection(lastWillTopic, qualityOfService, 1, true, clientMode);

        mqttClientEngine.createPublisherConnection(lastWillTopic, qualityOfService, new String("Message").getBytes(), 1, 1, clientMode);

        ExecutorService clientControlSubscriptionThreads = Executors.newFixedThreadPool(2);

        MQTTClientConnectionConfiguration configuration = new MQTTClientConnectionConfiguration();

        configuration.setBrokerHost(MQTTConstants.BROKER_HOST);
        configuration.setBrokerProtocol(MQTTConstants.BROKER_PROTOCOL);
        configuration.setBrokerPort(MQTTConstants.BROKER_PORT);
        configuration.setBrokerPassword(MQTTConstants.BROKER_PASSWORD);
        configuration.setBrokerUserName(MQTTConstants.BROKER_USER_NAME);
        configuration.setCleanSession(true);

        // Create mqtt client
        MQTTBlockingSubscriberClient mqttClient = new MQTTBlockingSubscriberClient(configuration, "2345" , lastWillTopic, qualityOfService, true);
        clientControlSubscriptionThreads.execute(mqttClient);

        try {
            Thread.sleep(1000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new Runnable() {
            @Override
            public void run() {


            }
        });

        executorService.shutdown();
        try {
            Thread.sleep(5000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<MqttMessage> receivedMessages = mqttClientEngine.getReceivedMessages();

        mqttClientEngine.shutdown();

        Assert.assertEquals(receivedMessages.size(), 2, "The received message count is incorrect.");

        Assert.assertEquals(new String(String.valueOf(receivedMessages.get(1))), lastWillMessage,
                "The received message is incorrect");
    }
}
