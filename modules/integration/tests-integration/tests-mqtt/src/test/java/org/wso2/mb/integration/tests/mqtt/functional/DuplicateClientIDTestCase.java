package org.wso2.mb.integration.tests.mqtt.functional;

import org.apache.commons.lang.RandomStringUtils;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesMQTTClient;
import org.wso2.mb.integration.common.clients.ClientMode;
import org.wso2.mb.integration.common.clients.MQTTClientConnectionConfiguration;
import org.wso2.mb.integration.common.clients.MQTTClientEngine;
import org.wso2.mb.integration.common.clients.MQTTConstants;
import org.wso2.mb.integration.common.clients.QualityOfService;
import org.wso2.mb.integration.common.clients.operations.mqtt.async.MQTTAsyncSubscriberClient;
import org.wso2.mb.integration.common.clients.operations.mqtt.blocking.MQTTBlockingSubscriberClient;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import javax.xml.xpath.XPathExpressionException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test case to validate the MQTT broker behaviour when connecting 2 clients with same client IDs.
 * Expectation : client1 should be disconnected upon arrival of client2, and client2 should then be connected.
 */
public class DuplicateClientIDTestCase extends MBIntegrationBaseTest {

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
     * Send message and receive for a long topic hierarchy.
     */
    @Test(groups = {"wso2.mb", "mqtt"}, description = "Send message and receive for a long topic hierarchy")
    public void performDuplicateClientIDTestCase() throws MqttException, XPathExpressionException, InterruptedException {

        String dupClientID = "DUP_" + RandomStringUtils.random(MQTTConstants.CLIENT_ID_LENGTH - 4, String.valueOf(System
                .currentTimeMillis()));

        String dupTopicName = "duplicateTopicA";

        MQTTClientEngine mqttClientEngine = null;

        try {

            mqttClientEngine = new MQTTClientEngine();
            MQTTClientConnectionConfiguration clientConfig = mqttClientEngine.getConfigurations(automationContext);

            mqttClientEngine.createSubscriberConnection(clientConfig, dupTopicName, QualityOfService.LEAST_ONCE, true,
                    ClientMode.ASYNC, dupClientID);

            TimeUnit.MILLISECONDS.sleep(MQTTConstants.CLIENT_CONNECT_TIMEOUT);

            Assert.assertTrue(mqttClientEngine.getSubscriberList().get(0).isConnected(),
                    "Client 1 has not yet connected, or has been disconnected.");

            mqttClientEngine.createSubscriberConnection(clientConfig, dupTopicName, QualityOfService.LEAST_ONCE, true,
                    ClientMode.ASYNC, dupClientID);

            TimeUnit.MILLISECONDS.sleep(MQTTConstants.CLIENT_CONNECT_TIMEOUT);

            Assert.assertFalse(mqttClientEngine.getSubscriberList().get(0).isConnected(), "Due to Client 2, client 1 should be disconnected, " +
                    "but it is not.");

            Assert.assertTrue(mqttClientEngine.getSubscriberList().get(1).isConnected(), "Client 2 has not yet connected, or has been disconnected.");

        } finally {
            if (null != mqttClientEngine) {
                mqttClientEngine.shutdown();
                log.info("clients are disconnected");
            }
        }

    }
}
