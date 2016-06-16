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

package org.wso2.carbon.andes.transports.mqtt.netty.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.mqtt.adaptors.MessagingAdaptor;
import org.wso2.carbon.andes.transports.mqtt.broker.BrokerVersion;
import org.wso2.carbon.andes.transports.mqtt.broker.Command;
import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.ConnectMessage;
import org.wso2.carbon.andes.transports.server.Broker;
import org.wso2.carbon.andes.transports.server.BrokerException;


/**
 * <p>
 * The decoded message will be handled through this
 * 1. The message action will be identified
 * 2. Based on the action the relevant services of the distribution will be invoked/handed over
 * </p>
 * <p>
 * <b>Note : </b> For each channel/connection a new instance of messaging handler will be created
 * </p>
 */
public class MqttMessagingHandler extends ChannelInboundHandlerAdapter {

    /**
     * Creates a manager to manage between connections
     */
    MqttChannel channel = null;


    private static Log log = LogFactory.getLog(MqttMessagingHandler.class);


    /**
     * The decoded message will be interpreted and multiplexed
     *
     * @param ctx contains the channel context the message is originated
     * @param msg the interpreted bytes message
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        AbstractMessage mqttMessage = (AbstractMessage) msg;

        //If the channel is being read for the first time a new channel will be created
        if (null == channel) {
            channel = new MqttChannel(ctx);
        }

        //If this is the connection message we need to register the protocol version supported through the channel
        if (null == channel.getVersion()) {
            if (log.isDebugEnabled()) {
                log.debug("Initializing the MQTT channel with the connection details.");
            }
            //This means the incoming message is a connection message
            //Will extract the version and register that in the channel
            ConnectMessage connectMessage = (ConnectMessage) msg;
            BrokerVersion brokerVersion = BrokerVersion.getBrokerVersion(connectMessage.getProcotolVersion());
            channel.setVersion(brokerVersion);
        }

        //Will identify where the message should flow
        Broker broker = channel.getVersion().getBroker();
        MessagingAdaptor adaptor = channel.getVersion().getAdaptor();
        Command.getCommand(mqttMessage.getMessageType()).process(broker, mqttMessage, channel, adaptor);

    }

    /**
     * Defines the routine when channel is in-active
     *
     * @param ctx holds the context information relevant for the inactive channel
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Netty Channel " + ctx.name() + " got inactive");
        // MqttChannel channel = channelManager.removeChannel(ctx);
        if (null != channel) {
            MessagingAdaptor adaptor = channel.getVersion().getAdaptor();
            channel.getVersion().getBroker().disconnect(null, channel, adaptor);
            //brokerFactory.getBroker(channel.getVersion()).disconnect(null, channel);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("The channel " + ctx.name() + " is already disconnected.");
            }
        }
    }

    /**
     * Defines the routine when an exception occurs
     *
     * @param ctx   holds the context information relevant for the inactive channel
     * @param cause the reason of the triggered exception
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String error = "An error occurred in Netty Channel ";
        log.error(error, cause);
        throw new BrokerException(error, cause);
    }
}
