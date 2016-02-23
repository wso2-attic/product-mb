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

package org.wso2.carbon.andes.transports.mqtt.commands.v300;


import io.netty.handler.timeout.IdleStateHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.mqtt.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.MqttConstants;
import org.wso2.carbon.andes.transports.mqtt.connectors.IConnector;
import org.wso2.carbon.andes.transports.mqtt.exceptions.ConnectorException;
import org.wso2.carbon.andes.transports.mqtt.handlers.IdleStateEventMqttHandler;
import org.wso2.carbon.andes.transports.mqtt.protocol.Utils;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.ConnAckMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.ConnectMessage;
import org.wso2.carbon.andes.transports.server.BrokerException;

/**
 * Defines the set of phases the connection should go through, implemented based on interface influence
 */
public class Connect {

    private static final Log log = LogFactory.getLog(Connect.class);

    /**
     * Validates whether the provided protocol version is supported
     *
     * @throws BrokerException
     */
    public static boolean validateProtocolVersion(ConnectMessage message, ConnAckMessage ack) throws BrokerException {
        boolean success;

        //TODO move this to 3.1.1 package for 3.1.1 validation
        if (message.getProcotolVersion() != Utils.VERSION_3_1 &&
                message.getProcotolVersion() != Utils.VERSION_3_1_1) {
            ack.setReturnCode(ConnAckMessage.UNNACEPTABLE_PROTOCOL_VERSION);
            String error = "The protocol version provided as " + message.getProcotolVersion() + " is not compatible";
            log.error(error);
            throw new BrokerException(error);
        } else {
            success = true;
        }
        return success;
    }

    /**
     * Validates the client identifier conforms to the expected standard
     *
     * @throws BrokerException
     */
    public static boolean validateClientIdentifier(ConnectMessage message, ConnAckMessage ack) throws BrokerException {

        boolean success;
        //TODO also need to check whehter there're existing clients with the same id
        //If exists need to remove the old session as per the spec
        if (message.getClientID() == null || message.getClientID().length() == 0) {
            ack.setReturnCode(ConnAckMessage.IDENTIFIER_REJECTED);
            String error = "The client identifier information provided as is invalid";
            log.error(error);
            throw new BrokerException(error);
        } else {
            success = true;
        }

        return success;
    }

    /**
     * <p>
     * Authenticates the credentials provided in the connection message
     * </p>
     * <p>
     * <b>Note :</b> this step is optional, servers may chose not to use authentication
     * </p>
     *
     * @throws BrokerException
     */
    public static boolean authenticate(ConnectMessage message, ConnAckMessage ack) throws BrokerException {
        boolean success;
        //By protocol the authentication will occur only if the user flag is set to 'true'
        //If the server expects to authenticate and the user does not provide the credentials that's invalid
        //TODO need to pass the authenticator here so that the user/password would be validated
        if (!message.isUserFlag()) {
            ack.setReturnCode(ConnAckMessage.BAD_USERNAME_OR_PASSWORD);
            String error = "The user credentials provided are invalid";
            log.error(error);
            throw new BrokerException(error);
        } else {
            success = true;
        }
        return success;
    }


    /**
     * Re define the channel with the keep alive params specified by the client
     *
     * @throws BrokerException
     */
    public static boolean processKeepAlive(ConnectMessage message, MqttChannel channel) throws BrokerException {
        boolean success;
        //First the current idle state handlers will be removed
        //TODO define constant
        removeHandler("idleStateMQTTHandler", channel);
        removeHandler("idleStateMQTTEventHandler", channel);

        //Then we re-fresh the handlers with the new keep alive values
        int keepAliveValue = message.getKeepAlive();
        int roundKeepAliveValue = Math.round(keepAliveValue * 1.5f);
        channel.getChannel().pipeline().addFirst("idleStateMQTTHandler",
                new IdleStateHandler(0, 0, roundKeepAliveValue));
        //TODO check if its neccessary to do this step and reinit the Event handler
        channel.getChannel().pipeline().addAfter("idleStateMQTTHandler", "idleStateMQTTEventHandler",
                new IdleStateEventMqttHandler());

        success = true;

        return success;
    }

    /**
     * Processors the will flag in the message
     *
     * @throws BrokerException
     */
    public static boolean processWilFlag() throws BrokerException {
        boolean success = false;
        log.warn("The message contained a will flag, this will be ignored");
        return success;
    }

    /**
     * saves the relevant properties in the channel to maintain state
     *
     * @return Connect
     */
    public static boolean saveChannelState(ConnectMessage message, MqttChannel channel) {
        boolean success;

        channel.addProperty(MqttConstants.SESSION_DURABILITY_PROPERTY_NAME,
                String.valueOf(message.isCleanSession()));
        channel.addProperty(MqttConstants.CLIENT_ID_PROPERTY_NAME, message.getClientID());
        channel.addProperty(MqttConstants.KEEP_ALIVE_PROPERTY_NAME, String.valueOf(message.getKeepAlive()));

        success = true;
        return success;
    }

    /**
     * Finalized the flow if all is well the connection will be returned as accepted
     */
    public static boolean complete(ConnectMessage message, ConnAckMessage ack) {
        boolean success;
        if (log.isDebugEnabled()) {
            log.debug("Connect accepted for client " + message.getClientID());
        }
        ack.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);
        success = true;
        return success;
    }

    /**
     * persists the message in the specified store
     *
     * @param messageStore specifies the message store which will be informed about the connect message
     */
    public boolean notifyStore(IConnector messageStore, ConnectMessage message) throws BrokerException {
        boolean success;

        try {
            messageStore.storeConnection(message);
            success = true;
        } catch (ConnectorException e) {
            //This means the connection is not successful
            String errorMessage = "Error occurred while registering the connection with the store";
            throw new BrokerException(errorMessage, e);
        }
        return success;
    }

    /**
     * Removes a given handler by looking it up from the pipeline
     *
     * @param name the name of the handler defined when server is initialized
     */
    private static void removeHandler(String name, MqttChannel channel) {
        if (channel.getChannel().pipeline().names().contains(name)) {
            channel.getChannel().pipeline().remove(name);
        }
    }

}
