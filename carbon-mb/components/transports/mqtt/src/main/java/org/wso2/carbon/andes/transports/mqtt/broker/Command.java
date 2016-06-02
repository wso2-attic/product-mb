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

package org.wso2.carbon.andes.transports.mqtt.broker;

import org.wso2.carbon.andes.transports.mqtt.adaptors.MessagingAdaptor;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.PingRespMessage;
import org.wso2.carbon.andes.transports.server.Broker;
import org.wso2.carbon.andes.transports.server.BrokerException;

import java.util.HashMap;
import java.util.Map;

/**
 * Would identify the sub-routine of the message based on the specified command
 */
@SuppressWarnings("unused")
public enum Command {

    CONNECT(AbstractMessage.CONNECT) {
        @Override
        public void process(Broker broker, AbstractMessage msg, MqttChannel callback,
                            MessagingAdaptor messageAdaptor) throws BrokerException {
            broker.connect(msg, callback);
        }
    },
    DISCONNECT(AbstractMessage.DISCONNECT) {
        @Override
        public void process(Broker broker, AbstractMessage msg, MqttChannel callback,
                            MessagingAdaptor messageAdaptor) throws BrokerException {
            broker.disconnect(msg, callback, messageAdaptor);
        }
    },
    PUBLISH(AbstractMessage.PUBLISH) {
        @Override
        public void process(Broker broker, AbstractMessage msg, MqttChannel callback,
                            MessagingAdaptor messageAdaptor) throws BrokerException {
            broker.publish(msg, callback, messageAdaptor);
        }
    },
    PUBLISHER_ACK(AbstractMessage.PUBACK) {
        @Override
        public void process(Broker broker, AbstractMessage msg, MqttChannel callback,
                            MessagingAdaptor messageAdaptor) throws BrokerException {
            broker.subAck(msg, callback, messageAdaptor);
        }
    },
    PUBLISHER_COMPLETE(AbstractMessage.PUBCOMP) {
        @Override
        public void process(Broker broker, AbstractMessage msg, MqttChannel callback,
                            MessagingAdaptor messageAdaptor) throws BrokerException {
            broker.subAck(msg, callback, messageAdaptor);
        }
    },
    PUBLISHER_RECEIVED(AbstractMessage.PUBREC) {
        @Override
        public void process(Broker broker, AbstractMessage msg, MqttChannel callback,
                            MessagingAdaptor messageAdaptor) throws BrokerException {
            broker.subAck(msg, callback, messageAdaptor);
        }
    },
    PUBLISHER_RELEASE(AbstractMessage.PUBREL) {
        @Override
        public void process(Broker broker, AbstractMessage msg, MqttChannel callback,
                            MessagingAdaptor messageAdaptor) throws BrokerException {
            broker.pubAck(msg, callback, messageAdaptor);
        }
    },
    SUBSCRIBE(AbstractMessage.SUBSCRIBE) {
        @Override
        public void process(Broker broker, AbstractMessage msg, MqttChannel callback,
                            MessagingAdaptor messagingAdaptor) throws BrokerException {
            broker.subscribe(msg, callback, messagingAdaptor);
        }
    },
    UN_SUBSCRIBE(AbstractMessage.UNSUBSCRIBE) {
        @Override
        public void process(Broker broker, AbstractMessage msg, MqttChannel callback,
                            MessagingAdaptor messagingAdaptor) throws BrokerException {
            broker.unSubscribe(msg, callback, messagingAdaptor);
        }
    },
    PINGREQ(AbstractMessage.PINGREQ) {
        @Override
        public void process(Broker broker, AbstractMessage msg, MqttChannel callback,
                            MessagingAdaptor messagingAdaptor) throws BrokerException {
            broker.nack(callback, messagingAdaptor);
            PingRespMessage message = new PingRespMessage();
            callback.write(message);
        }
    };

    /**
     * We add the specific commands and the messages to the map
     */
    private static final Map<Byte, Command> commandMessages = new HashMap<>();

    /**
     * We define a static initializer to populate values to the map
     */
    static {
        for (Command command : Command.values()) {
            commandMessages.put(command.getCommandValue(), command);
        }
    }

    /**
     * Holds the value of the command messages in its byte representation
     */
    private byte commandValue;

    Command(byte command) {
        this.commandValue = command;
    }

    public byte getCommandValue() {
        return this.commandValue;
    }

    /**
     * Get the command routine
     *
     * @param command the byte representation of the command
     * @return the enum representation of the command
     */
    public static Command getCommand(byte command) {
        return commandMessages.get(command);
    }

    /**
     * Based on the message type the corresponding subroutine would be called
     *
     * @param broker         the the implementation of the protocol  i.e MQTT 3.1, MQTT 3.1.1
     * @param msg            message which corresponds to the command
     * @param callback       the channel which the command was sent
     * @param messageAdaptor the adopter interface the broker connects for storage/distribution
     * @throws BrokerException
     */
    public abstract void process(Broker broker, AbstractMessage msg, MqttChannel callback, MessagingAdaptor
            messageAdaptor) throws BrokerException;
}
