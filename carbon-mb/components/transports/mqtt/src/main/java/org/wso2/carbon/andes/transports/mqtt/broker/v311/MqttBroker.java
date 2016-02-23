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

package org.wso2.carbon.andes.transports.mqtt.broker.v311;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.mqtt.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.MqttConstants;
import org.wso2.carbon.andes.transports.mqtt.commands.v300.Connect;
import org.wso2.carbon.andes.transports.mqtt.commands.v300.Disconnect;
import org.wso2.carbon.andes.transports.mqtt.commands.v300.PingRequest;
import org.wso2.carbon.andes.transports.mqtt.commands.v300.Publish;
import org.wso2.carbon.andes.transports.mqtt.commands.v300.PublisherAck;
import org.wso2.carbon.andes.transports.mqtt.commands.v300.PublisherComplete;
import org.wso2.carbon.andes.transports.mqtt.commands.v300.PublisherReceived;
import org.wso2.carbon.andes.transports.mqtt.commands.v300.PublisherRelease;
import org.wso2.carbon.andes.transports.mqtt.commands.v300.Subscribe;
import org.wso2.carbon.andes.transports.mqtt.commands.v300.UnSubscribe;
import org.wso2.carbon.andes.transports.mqtt.connectors.IConnector;
import org.wso2.carbon.andes.transports.mqtt.distribution.bridge.AndesConnector;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.AbstractMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.ConnAckMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.ConnectMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PubAckMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PubCompMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PubRecMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PubRelMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PublishMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.SubscribeMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.SubscribeMessage.Couple;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.UnsubscribeMessage;
import org.wso2.carbon.andes.transports.server.Broker;
import org.wso2.carbon.andes.transports.server.BrokerException;

import java.util.Iterator;
import java.util.List;

/**
 * Defines the connection between the MQTT message decoding to handling brokering operation
 */
public class MqttBroker implements Broker {

    private static final Log log = LogFactory.getLog(MqttBroker.class);

    private IConnector messageStore = new AndesConnector();

    /**
     * <p>
     * Connects with the broker to exchange MQTT topic messages
     * </p>
     * <b>{@inheritDoc}</b>
     */
    @Override
    public void connect(AbstractMessage msg, MqttChannel callback)
            throws BrokerException {
        ConnectMessage connectMessage = (ConnectMessage) msg;
        log.info("MQTT Connect initiated with MQTT broker for client ID  " +
                connectMessage.getClientID() + " for user "
                + connectMessage.getUsername() + " with keep alive set to "
                + connectMessage.getKeepAlive());

        ConnAckMessage connAckMessage = new ConnAckMessage();

        try {
            Connect.validateProtocolVersion(connectMessage, connAckMessage);
            Connect.validateClientIdentifier(connectMessage, connAckMessage);
            Connect.processKeepAlive(connectMessage, callback);
            Connect.processWilFlag();
            Connect.saveChannelState(connectMessage, callback);
            Connect.complete(connectMessage, connAckMessage);
        } catch (BrokerException e) {
            callback.close();
            throw e;
        } finally {
            callback.write(connAckMessage);
            log.info("Connect acknowledgment was sent to " + connectMessage.getClientID());
        }

    }


    /**
     * <p>
     * Disconnects a particular MQTT channel from a broker
     * </p>
     * <b>{@inheritDoc}</b>
     */
    @Override
    public void disconnect(AbstractMessage msg, MqttChannel channel) throws
            BrokerException {
        log.info("Disconnect request was sent from the client " + channel.getProperty(MqttConstants
                .CLIENT_ID_PROPERTY_NAME));
        try {
            Disconnect.notifyStore(messageStore, channel);
        } finally {
            channel.close();
        }
    }


    /**
     * <p>
     * Un-Subscribes from a given MQTT topic/s
     * </p>
     * <b>{@inheritDoc}</b>
     */
    @Override
    public void unSubscribe(AbstractMessage msg, MqttChannel callback) throws
            BrokerException {
        log.info("Un subscribe request was sent from the client " + callback.getProperty(MqttConstants
                .CLIENT_ID_PROPERTY_NAME));
        UnsubscribeMessage message = (UnsubscribeMessage) msg;
        UnSubscribe.notifyStore(messageStore, message, callback);
    }

    /**
     * <p>
     * Subscribes an MQTT topic/s with the broker
     * The callback would identify topic events and feedback to channels
     * </p>
     * <b>{@inheritDoc}</b>
     */

    @Override
    public void subscribe(AbstractMessage msg, MqttChannel callback) throws BrokerException {
        SubscribeMessage subscribeMessage = (SubscribeMessage) msg;

        //Subscribe message will contain multiple topic filters, we would print them up by increasing the log
        //severity
        if (log.isDebugEnabled()) {
            List<Couple> subscriptions = subscribeMessage.subscriptions();
            StringBuilder topicList = new StringBuilder();
            for (Iterator<Couple> subscriptionItr = subscriptions.iterator(); subscriptionItr.hasNext(); ) {
                Couple subscription = subscriptionItr.next();
                topicList.append('|').append(subscription.getTopicFilter());
            }

            log.debug("The subscription contains the following topic filters " + topicList);
        }


        Subscribe.notifyStore(messageStore, subscribeMessage, callback);

    }

    /**
     * <p>
     * Provides the published MQTT message for the broker
     * </p>
     * <b>{@inheritDoc}</b>
     */

    @Override
    public void publish(AbstractMessage msg, MqttChannel callback) throws BrokerException {
        PublishMessage message = (PublishMessage) msg;
        if (log.isDebugEnabled()) {
            log.debug("Message published from channel " + callback.getProperty(MqttConstants.CLIENT_ID_PROPERTY_NAME) +
                    " with message id " + message.getMessageID() + " qos " + message.getQos() + " for topic " + message
                    .getTopicName());
        }

        Publish.notifyStore(messageStore, message, callback);
    }

    /**
     * <p>
     * Triggers when a publisher acknowledges for a given topic message
     * For MQTT this will trigger when PUBREL is sent by the publisher
     * </p>
     * <b>{@inheritDoc}</b>
     */
    @Override
    public void pubAck(AbstractMessage msg, MqttChannel callback) throws BrokerException {
        //Need to process the message type
        switch (msg.getMessageType()) {
            case AbstractMessage.PUBREL:
                PubRelMessage message = (PubRelMessage) msg;
                PublisherRelease.notifyStore(message, callback);
                break;
            default:
                break;
        }
    }

    /**
     * <p>
     * Triggers when an acknowledgment is sent by a subscriber to a message
     * For MQTT this will trigger when PUBREC, PUBCOMP, SUBACK is send by the subscribers
     * </p>
     * <b>{@inheritDoc}</b>
     */
    @Override
    public void subAck(AbstractMessage msg, MqttChannel callback) throws BrokerException {
        switch (msg.getMessageType()) {
            case AbstractMessage.PUBREC:
                PubRecMessage pubRecMessage = (PubRecMessage) msg;
                PublisherReceived.notifyStore(messageStore, pubRecMessage, callback);
                break;
            case AbstractMessage.PUBCOMP:
                PubCompMessage pubCompMessage = (PubCompMessage) msg;
                PublisherComplete.notifyStore(pubCompMessage, callback);
                break;
            case AbstractMessage.PUBACK:
                PubAckMessage publisherAckMessage = (PubAckMessage) msg;
                PublisherAck.notifyStore(messageStore, publisherAckMessage, callback);
                break;
            default:
                break;
        }

    }

    /**
     * Specifies to invoke
     */
    @Override
    public void nack(MqttChannel callback) throws BrokerException {
        PingRequest.notifyStore(messageStore, callback);
    }


}
