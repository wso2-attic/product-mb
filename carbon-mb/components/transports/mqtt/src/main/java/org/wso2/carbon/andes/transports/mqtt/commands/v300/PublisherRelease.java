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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.mqtt.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.MqttConstants;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PubCompMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PubRelMessage;
import org.wso2.carbon.andes.transports.server.BrokerException;

/**
 * Acknowledgment sent from the publisher to the broker for QoS 2 messages
 */
public class PublisherRelease {

    private static final Log log = LogFactory.getLog(PublisherRelease.class);

    /**
     * Notifies the message store on receiving a published message
     *
     * @param channel specifies the connection information of the client
     * @return
     */
    public static boolean notifyStore(PubRelMessage message, MqttChannel channel) throws BrokerException {
        //When publisher releases the message should be discarded from the store
        channel.getPublisherAckWriter().releaseMessage(message.getMessageID(),
                channel.getProperty(MqttConstants.CLIENT_ID_PROPERTY_NAME));
        //Then we need to send the publisher comp for the publisher
        PubCompMessage pubCompMessage = new PubCompMessage();
        pubCompMessage.setMessageID(message.getMessageID());
        channel.write(pubCompMessage);

        return true;
    }

}
