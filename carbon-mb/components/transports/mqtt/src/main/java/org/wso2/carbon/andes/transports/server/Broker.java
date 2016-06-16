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

package org.wso2.carbon.andes.transports.server;

import org.wso2.carbon.andes.transports.mqtt.adaptors.MessagingAdaptor;
import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;

/**
 * Handles polymorphic behaviour represented through each version of the MQTT protocol
 *
 * @param <K> defines the message adopter the broker will be connecting
 */
public interface Broker<K extends MessagingAdaptor> {
    /**
     * Triggers the connectivity with the broker
     *
     * @param msg      the details of the message connectivity
     * @param callback used to send the CONNACK
     * @throws BrokerException
     */
    void connect(AbstractMessage msg, MqttChannel callback) throws BrokerException;

    /**
     * Triggers when disconnected with the broker
     *
     * @param msg            the details of the message which holds the disconnection
     * @param channel        provides the details of the client connection which should be disconnected
     * @param messageAdaptor the underlying broker store/distribution layer
     * @throws BrokerException
     */
    void disconnect(AbstractMessage msg, MqttChannel channel, K messageAdaptor) throws BrokerException;

    /**
     * Triggers when a client sends a subscriber message
     *
     * @param msg            the details of the message which holds the un-callback
     * @param callback       internal channel used to send the SUBACK and message
     * @param messageAdaptor the underlying broker store/distribution layer
     * @throws BrokerException
     */
    void subscribe(AbstractMessage msg, MqttChannel callback, K messageAdaptor) throws BrokerException;

    /**
     * Triggers when un-subscribed from the broker
     *
     * @param msg            the details of the message when un-subscribed
     * @param callback       used to send the UNSUBACK
     * @param messageAdaptor the underlying broker store/distribution layer
     * @throws BrokerException
     */
    void unSubscribe(AbstractMessage msg, MqttChannel callback, K messageAdaptor) throws BrokerException;

    /**
     * Triggers when a message is published
     *
     * @param msg            the details of the message when a message is published
     * @param callback       used to send the PUBACK, this will be the most immediate ack
     * @param messageAdaptor the underlying broker store/distribution layer
     * @throws BrokerException
     */
    void publish(AbstractMessage msg, MqttChannel callback, K messageAdaptor) throws BrokerException;

    /**
     * Triggers each time when a publisher acknowledgment is received
     *
     * @param msg            details of the message when publisher acknowledges
     * @param callback       will be used to send the acknowledgment to the sender
     * @param messageAdaptor the underlying broker store/distribution layer
     * @throws BrokerException
     */
    void pubAck(AbstractMessage msg, MqttChannel callback, K messageAdaptor) throws BrokerException;

    /**
     * @param msg            the message which contains the subscription acknowledgments
     * @param callback       the channel which will allow the server to add correspondence
     * @param messageAdaptor the underlying broker store/distribution layer
     * @throws BrokerException
     */
    void subAck(AbstractMessage msg, MqttChannel callback, K messageAdaptor) throws BrokerException;

    /**
     * Negative acknowledgment or specifying to process un acknowledged messages
     *
     * @param callback       specifies the channel information
     * @param messageAdaptor the underlying broker store/distribution layer
     * @throws BrokerException
     */
    void nack(MqttChannel callback, K messageAdaptor) throws BrokerException;

}
