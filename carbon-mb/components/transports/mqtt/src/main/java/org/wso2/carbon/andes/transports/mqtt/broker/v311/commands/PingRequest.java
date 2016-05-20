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

package org.wso2.carbon.andes.transports.mqtt.broker.v311.commands;


import org.wso2.carbon.andes.transports.mqtt.adaptors.MessagingAdaptor;
import org.wso2.carbon.andes.transports.mqtt.adaptors.common.MessageDeliveryTag;
import org.wso2.carbon.andes.transports.mqtt.adaptors.exceptions.AdaptorException;
import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;
import org.wso2.carbon.andes.transports.server.BrokerException;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;


/**
 * Processors the ping request which is received through the client, we used the ping request to identify unacked
 * messages
 */
public class PingRequest {
    /**
     * Notifies the message store on receiving a published message
     *
     * @param channel specifies the connection information of the client
     * @return Subscribe
     */
    public static boolean notifyStore(MessagingAdaptor messageStore, MqttChannel channel)
            throws
            BrokerException {
        //First need to get the set of message which have not being acked, This will be marked through the reference
        // counter maintained in the message delivery information
        Set<Map.Entry<MessageDeliveryTag, Long>> undeliveredTags = channel.getMessageDeliveryTagMap()
                .getUndeliveredTags();
        //We need to update the traversal count of the delivery tag id
        //If the reference count has reached the thresh-hold we need to clear the transport state and hand it over to
        //the connector to retry the unacked message
        for (Iterator<Map.Entry<MessageDeliveryTag, Long>> iterator = undeliveredTags.iterator(); iterator.hasNext();
                ) {
            Map.Entry<MessageDeliveryTag, Long> next = iterator.next();
            MessageDeliveryTag messageDeliveryTag = next.getKey();
            Long relatedMessageId = next.getValue();
            messageDeliveryTag.incrementTraversalCount();
            //We have the dataset sorted according to the traversal count, hence we could guarantee the most eligible
            // message ids which should be notified to be retried will be in the first entries.
            if (messageDeliveryTag.getKeepAliveTraversal() >= 2) {
               // UUID channelID = UUID.fromString(channel.getProperty(MqttUtils.CLUSTER_SUB_ID_PROPERTY_NAME));
                //if the message id has being traversed more than 2 time the message will be notified to be retried
                //TODO notify andes
                try {
                    messageStore.storeRejection(messageDeliveryTag, channel);
                } catch (AdaptorException e) {
                    String error = "Error occurred while rejecting the message " + relatedMessageId + " " +
                            "which relates to " + messageDeliveryTag.getMessageId();
                    throw new BrokerException(error, e);
                } finally {
                    //We clear the state maintained by the channel, considering an ack was received for the message
                    //This message will be retried by the server, even if an exception occurs we clear the state
                    //since we inform the user on the issue and we do not want state inconsistencies
                    channel.getMessageDeliveryTagMap().ackReceived(messageDeliveryTag);
                }


            } else {
                //The message should go through another traversal
                //TODO include a debug log here
            }

        }

        return true;

    }
}
