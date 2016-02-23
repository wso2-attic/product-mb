package org.wso2.carbon.andes.transports.mqtt;

import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * Manages the channels, performs the channel removal and insertion
 * We use dependency injection here instead of using singleton
 * </P>
 * <p>
 * <b>Note :</b> The channels should be persisted, since the state of the channel would require to be validated for
 * each command message i.e CONNECT message holds the client id, session attributes which needs processing for
 * other command messages such as SUBSCRIPTION
 * </P>
 */
public class MqttChannelManager {

    /**
     * Maintain the channel state, in order to link between the command messages
     * i.e information sent through the CONNECT will be required for SUBSCRIPTION, PUBLISHING etc
     */

    private Map<ChannelHandlerContext, MqttChannel> channels = new ConcurrentHashMap<>();

    /**
     * Handles delivery of the messages, by generating a message ID for, messages sent to subscribers
     */
  //  private MessageDeliveryTagScannerFactory deliveryTagFactory = new MessageDeliveryTagScannerFactory();

    /**
     * Will add a channel if a channel does not already exist
     *
     * @param ctx the context which holds the channel information
     * @return MqttChannel the available/newly created channel
     */
    public MqttChannel addChannel(ChannelHandlerContext ctx) {
        MqttChannel channel = channels.get(ctx);
        if (null == channel) {
           // channel = new MqttChannel(ctx, deliveryTagFactory);
            channels.put(ctx, channel);
        }

        return channel;
    }

    /**
     * Removes the channel off the list, this will be called when a connection is closed or disconnected
     *
     * @param ctx holds information relevant to the channel
     * @return MqttChannel returns an MQTT channel for the provided context
     */
    public MqttChannel removeChannel(ChannelHandlerContext ctx) {
        return channels.remove(ctx);
    }

}
