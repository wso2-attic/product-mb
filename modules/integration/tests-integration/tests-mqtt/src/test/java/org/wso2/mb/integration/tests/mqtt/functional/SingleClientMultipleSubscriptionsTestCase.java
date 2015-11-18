/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.mb.integration.common.clients.*;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.xml.xpath.XPathExpressionException;

/**
 * Test case to verify subscribing to multiple topics with the same client.
 */
public class SingleClientMultipleSubscriptionsTestCase extends MBIntegrationBaseTest {

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
     * Subscribe to two topics with the same client and verify whether messages are received from both topics.
     *
     * @throws MqttException
     * @throws XPathExpressionException
     */
    @Test
    public void performSingleClientMultipleSubscriptionsTest() throws MqttException, XPathExpressionException {
        String topic1 = "singleClientMultipleSubscriptions1";
        String topic2 = "singleClientMultipleSubscriptions2";
        int noOfMessages = 1;

        MQTTClientEngine mqttClientEngine = new MQTTClientEngine();
        //create the subscriber
        mqttClientEngine.createSubscriberConnection(topic1, QualityOfService.LEAST_ONCE, 1, false,
                ClientMode.BLOCKING, automationContext);

        AndesMQTTClient subscriber = mqttClientEngine.getSubscriberList().get(0);

        subscriber.subscribe(topic2);

        mqttClientEngine.createPublisherConnection(topic1, QualityOfService.LEAST_ONCE,
                MQTTConstants.TEMPLATE_PAYLOAD, 1,
                noOfMessages, ClientMode.BLOCKING, automationContext);

        mqttClientEngine.createPublisherConnection(topic2, QualityOfService.LEAST_ONCE,
                MQTTConstants.TEMPLATE_PAYLOAD, 1,
                noOfMessages, ClientMode.BLOCKING, automationContext);

        mqttClientEngine.waitUntilAllMessageReceivedAndShutdownClients();

        Assert.assertEquals(mqttClientEngine.getReceivedMessageCount(), noOfMessages * 2,
                "Did not receive expected message count ");


    }
}
