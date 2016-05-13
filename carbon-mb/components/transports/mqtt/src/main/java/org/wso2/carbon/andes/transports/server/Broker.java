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

import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;

/**
 * The abstraction between the protocol layer and the brokering
 * The following defines a set of services common to all the brokering transports
 * The following should be implemented to connect with the underlying brokering engine
 * Through this we could abstract the decoding with protocol semantics i.e having different semantics between different
 * versions
 */
public interface Broker {
    /**
     * Triggers the connectivity with the broker
     *
     * @param msg            the details of the message connectivity
     * @param callback       used to send the CONNACK
     * @throws BrokerException
     */
    void connect(AbstractMessage msg, MqttChannel callback) throws BrokerException;

    /**
     * Triggers when disconnected with the broker
     *
     * @param msg            the details of the message which holds the disconnection
     * @param channel        provides the details of the client connection which should be disconnected
     * @throws BrokerException
     */
    void disconnect(AbstractMessage msg, MqttChannel channel) throws BrokerException;

    /**
     * Triggers when callback is bound
     *
     * @param msg      the details of the message which holds the un-callback
     * @param callback internal channel used to send the SUBACK and message
     * @throws BrokerException
     */
    void subscribe(AbstractMessage msg, MqttChannel callback) throws BrokerException;

    /**
     * Triggers when un-subscribed from the broker
     *
     * @param msg      the details of the message when un-subscribed
     * @param callback used to send the UNSUBACK
     * @throws BrokerException
     */
    void unSubscribe(AbstractMessage msg, MqttChannel callback) throws BrokerException;

    /**
     * Triggers when a message is published
     *
     * @param msg      the details of the message when a message is published
     * @param callback used to send the PUBACK, this will be the most immediate ack
     * @throws BrokerException
     */
    void publish(AbstractMessage msg, MqttChannel callback) throws BrokerException;

    /**
     * Triggers each time when a publisher acknowledgment is received
     *
     * @param msg      details of the message when publisher acknowledges
     * @param callback will be used to send the acknowledgment to the sender
     * @throws BrokerException
     */
    void pubAck(AbstractMessage msg, MqttChannel callback) throws BrokerException;

    /**
     * @param msg      the message which contains the subscription acknowledgments
     * @param callback the channel which will allow the server to add correspondence
     * @throws BrokerException
     */
    void subAck(AbstractMessage msg, MqttChannel callback) throws BrokerException;

    /**
     * Negative acknowledgment or specifying to process un acknowledged messages
     *
     * @param callback specifies the channel information
     * @throws BrokerException
     */
    void nack(MqttChannel callback) throws BrokerException;

}
