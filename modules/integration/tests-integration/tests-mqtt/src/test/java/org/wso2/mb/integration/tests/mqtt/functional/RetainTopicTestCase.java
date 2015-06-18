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

package org.wso2.mb.integration.tests.mqtt.functional;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.ClientMode;

import org.wso2.mb.integration.common.clients.MQTTClientConnectionConfiguration;
import org.wso2.mb.integration.common.clients.MQTTClientEngine;
import org.wso2.mb.integration.common.clients.MQTTConstants;
import org.wso2.mb.integration.common.clients.QualityOfService;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;


import javax.xml.xpath.XPathExpressionException;
import java.util.List;

/**
 *
 * This test case will verify functionality of MQTT retain feature. Retain feature will keep last
 * retain enabled message from publisher, for future subscribers.
 *
 */
public class RetainTopicTestCase extends MBIntegrationBaseTest {



    /**
     * Initialize super class.
     *
     * @throws XPathExpressionException
     */
    @BeforeClass
    public void prepare() throws XPathExpressionException {
        super.init(TestUserMode.SUPER_TENANT_USER);
    }

    /**
     * Send a single mqtt message with retain enabled and receive.
     *
     * @throws org.eclipse.paho.client.mqttv3.MqttException
     */
    @Test(groups = {"wso2.mb", "mqtt"}, description = "Single mqtt retain message send receive test case")
    public void performSendReceiveRetainTopicTestCase() throws MqttException {
        String topic = "topic";
        int noOfSubscribers = 1;
        int noOfPublishers = 1;
        int noOfMessages = 1;
        boolean saveMessages = true;
        boolean retained = true;

        MQTTClientEngine mqttClientEngine = new MQTTClientEngine();

        MQTTClientConnectionConfiguration configuration = mqttClientEngine.getDefaultConfigurations();
        configuration.setRetain(retained);

        //create the subscriber to receive retain topic message
        mqttClientEngine.createSubscriberConnection(topic, QualityOfService.MOST_ONCE, noOfSubscribers,
                                                    saveMessages, ClientMode.BLOCKING);

        // create publisher to publish retain topic message
        mqttClientEngine.createPublisherConnection(topic, QualityOfService.MOST_ONCE,
                                                   MQTTConstants.TEMPLATE_PAYLOAD, noOfPublishers,
                                                   noOfMessages, ClientMode.BLOCKING, configuration);

        // wait until all messages received by subscriber and shut down clients.
        mqttClientEngine.waitUntilAllMessageReceivedAndShutdownClients();

        List<MqttMessage> receivedMessages = mqttClientEngine.getReceivedMessages();

        // Verify received messages equals to send message count.
        Assert.assertEquals(receivedMessages.size(), noOfMessages,
                            "The received message count is incorrect.");

        // Verify message payload has received correctly
        Assert.assertEquals(receivedMessages.get(0).getPayload(),
                            MQTTConstants.TEMPLATE_PAYLOAD, "The received message is incorrect");

    }



    /**
     * Send and receive single mqtt message with retain enabled. Subscriber will subscribe
     * after message been published.
     *
     * @throws org.eclipse.paho.client.mqttv3.MqttException
     */
    @Test(groups = {"wso2.mb", "mqtt"}, description = "Single mqtt retain message send receive test case")
    public void performSendReceiveRetainTopicForLateSubscriberTestCase() throws MqttException {
        String topic = "topic1";
        int noOfSubscribers = 1;
        int noOfPublishers = 1;
        int noOfMessages = 1;
        boolean saveMessages = true;
        boolean retained = true;

        MQTTClientEngine mqttClientEngine = new MQTTClientEngine();

        MQTTClientConnectionConfiguration configuration = mqttClientEngine.getDefaultConfigurations();
        configuration.setRetain(retained);

        //First, create publisher and publish retain topic message
        mqttClientEngine.createPublisherConnection(topic, QualityOfService.MOST_ONCE,
                                                   MQTTConstants.TEMPLATE_PAYLOAD, noOfPublishers,
                                                   noOfMessages, ClientMode.BLOCKING, configuration);


        //Finally,create the subscriber to receive retain topic message
        mqttClientEngine.createSubscriberConnection(topic, QualityOfService.MOST_ONCE, noOfSubscribers,
                                                    saveMessages, ClientMode.BLOCKING);

        // wait until all messages received by subscriber and shut down clients.
        mqttClientEngine.waitUntilAllMessageReceivedAndShutdownClients();

        List<MqttMessage> receivedMessages = mqttClientEngine.getReceivedMessages();

        // Verify received messages equals to send message count.
        Assert.assertEquals(receivedMessages.size(), noOfMessages,
                            "The received message count is incorrect.");

        // Verify message payload has received correctly
        Assert.assertEquals(receivedMessages.get(0).getPayload(),
                            MQTTConstants.TEMPLATE_PAYLOAD, "The received message is incorrect");

    }





}
