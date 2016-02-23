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
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PubCompMessage;
import org.wso2.carbon.andes.transports.server.BrokerException;

/**
 * Sent by the subscriber to the broker upon finalizing the submission of the message
 */
public class PublisherComplete {
    private static final Log log = LogFactory.getLog(PublisherComplete.class);

    /**
     * Notifies the message store on receiving a published message
     *
     * @param channel               specifies the connection information of the client
     * @return Subscribe
     */
    public static boolean notifyStore(PubCompMessage message, MqttChannel channel) throws
            BrokerException {
        //When the publisher complete is being received we release the state of the message from the list
        channel.getSubscriberAck().messageDeliveryCompleted(message.getMessageID());

        return true;
    }

}
