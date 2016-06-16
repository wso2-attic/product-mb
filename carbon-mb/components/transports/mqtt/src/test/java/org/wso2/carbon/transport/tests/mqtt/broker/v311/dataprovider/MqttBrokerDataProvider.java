/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.transport.tests.mqtt.broker.v311.dataprovider;

import org.testng.annotations.DataProvider;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.Utils;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.ConnAckMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.ConnectMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.PublishMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.SubscribeMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.UnsubscribeMessage;
import org.wso2.carbon.transport.tests.mqtt.broker.v311.dataprovider.commands.Message;

import java.nio.ByteBuffer;

/**
 * Specifies the data providers for Mqtt broker, used for unit testing
 */
public class MqttBrokerDataProvider {

    /**
     * Holds different combinations of connection commands which will be injected to the tests
     *
     * @return the list of connect messages
     */
    @DataProvider(name = "ConnectMessage")
    public static Object[][] getConnectionInformation() {

        Message mqttCleanSessionClient = createConnectMessage(true, "MqttCleanSessionClient", Utils
                .VERSION_3_1, ConnAckMessage.CONNECTION_ACCEPTED);
        Message mqttDurableClient = createConnectMessage(false, "MqttDurableClient", Utils.VERSION_3_1,
                ConnAckMessage.CONNECTION_ACCEPTED);

        Object[][] mockConnectionObjects = new Object[][]{{mqttCleanSessionClient}, {mqttDurableClient}};

        return mockConnectionObjects;
    }


    /**
     * Holds different combinations of disconnection commands
     *
     * @return list of disconnection messages
     */
    @DataProvider(name = "DisconnectMessage")
    public static Object[][] getDisconnectionInformation() {
        Message disconnectionMessage = createDisconnectionMessage();
        Object[][] mockDisconnectionObjects = new Object[][]{{disconnectionMessage}};
        return mockDisconnectionObjects;
    }

    /**
     * Holds different combinations of subscription message commands
     *
     * @return list of subscription messages
     */
    @DataProvider(name = "SubscribeMessage")
    public static Object[][] getSubscriptionInformation() {
        Message subscribeMessage = createSubscribeMessage();
        Object[][] mockSubscription = new Object[][]{{subscribeMessage}};
        return mockSubscription;
    }

    /**
     * Holds different combinations of un-subscription message commands
     *
     * @return list of un subscription messages
     */
    @DataProvider(name = "unSubscribeMessage")
    public static Object[][] getUnsubscriptionInformation() {
        Message unSubscribeMessage = createUnsubscribeMessage();
        Object[][] unSubscribe = new Object[][]{{unSubscribeMessage}};
        return unSubscribe;
    }

    /**
     * Holds different combinations of publish messages
     *
     * @return list of publish messages
     */
    @DataProvider(name = "PublishMessage")
    public static Object[][] getPublishInformation() {
        Message publishMessage = createPublishMessage();
        Object[][] publishMessageInformation = new Object[][]{{publishMessage}};
        return publishMessageInformation;
    }

    /**
     * Creates publish message
     *
     * @return mock publish message
     */
    private static Message createPublishMessage() {
        PublishMessage message = new PublishMessage();
        message.setPayload(ByteBuffer.wrap("TestMessage".getBytes()));
        message.setMessageID(1);
        message.setTopicName("testPublishTopic");
        message.setQos(AbstractMessage.QOSType.valueOf(0));

        Message publishMessage = new Message(message, null);

        return publishMessage;
    }

    /**
     * Creates a message which would un-subscribe a subscription from the list
     *
     * @return the message which holds the subscription
     */
    private static Message createUnsubscribeMessage() {

        UnsubscribeMessage unSubscribeMessage = new UnsubscribeMessage();
        unSubscribeMessage.addTopicFilter("unSubscriptionTestTopic");

        boolean shouldSubscriptionFail = true;
        Message unSubscribe = new Message(unSubscribeMessage, shouldSubscriptionFail);
        //We also need to create a subscription to un-subscribe
        addMockSubscriber("unSubscriptionTestTopic", "unSubsscriptionTestClient", "testUser", "true", "0", unSubscribe);

        return unSubscribe;
    }

    /**
     * Creates a subscription message
     *
     * @return message which holds the topic and the subscription
     */
    private static Message createSubscribeMessage() {
        int qos = 0;
        String topic = "testSubscriptionTopic";
        boolean dupFlag = true;
        int messageID = 1;
        int expectedResult = 1;

        SubscribeMessage subscribeMessage = new SubscribeMessage();
        Message message = new Message(subscribeMessage, expectedResult);
        SubscribeMessage.Couple subscription = new SubscribeMessage.Couple((byte) qos, topic);
        subscribeMessage.setDupFlag(dupFlag);
        subscribeMessage.setMessageID(messageID);
        subscribeMessage.addSubscription(subscription);


        return message;
    }

    /**
     * Creates disconnect messages
     *
     * @return the message require to form a disconnection
     */
    private static Message createDisconnectionMessage() {
        Message disconnection = new Message();
        addMockSubscriber("DisconnectionTestTopic", "MqttDisconnectClientId", "testUser", "true", "0", disconnection);
        return disconnection;
    }

    private static void addMockSubscriber(String topicFilter, String clientId, String userName, String session, String
            qos, Message message) {
        message.addMessageProperty("Client-ID", clientId);
        message.addMessageProperty("topicFilter", topicFilter);
        message.addMessageProperty("userName", userName);
        message.addMessageProperty("session", session);
        message.addMessageProperty("qosLevel", qos);
    }

    /**
     * Creates a MQTT connect message
     *
     * @param cleanSession    whether the message is durable
     * @param clientId        the unique id of the client which connect with the broker
     * @param protocolVersion the protocol version i.e 3.1v,3.1.1v
     * @param expected        the expected result of the message
     * @return the client mock connection
     */
    private static Message createConnectMessage(boolean cleanSession, String clientId, byte protocolVersion,
                                                byte expected) {
        ConnectMessage connectMessage = new ConnectMessage();
        connectMessage.setCleanSession(cleanSession);
        connectMessage.setClientID(clientId);
        connectMessage.setProcotolVersion(protocolVersion);
        //Finally we create the message which the expected outcome
        Message connection = new Message(connectMessage, expected);
        return connection;
    }
}


