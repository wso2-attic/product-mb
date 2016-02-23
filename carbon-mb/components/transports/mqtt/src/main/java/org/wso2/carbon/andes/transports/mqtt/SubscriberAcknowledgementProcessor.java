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

package org.wso2.carbon.andes.transports.mqtt;

import java.util.Set;
import java.util.TreeSet;

/**
 * Stores and processors acknowledgments received by QoS 2 messages
 * This will mainly maintain state of the messages which have sent a PUBREL
 * The following structure is a reverse of MessageDeliveryTagMap
 */
public class SubscriberAcknowledgementProcessor {
    /**
     * Holds the messages which are being acknowledged the state which will be maintained will be a unique identifier
     * for the message unique identifier would be messageId, the message state will be inserted upon sending a
     * publisher release for QoS 2 message, the state will be removed when a publisher complete is sent from the client
     */
    private Set<Integer> acknowledgedMessages = new TreeSet<>();

    /**
     * <p>
     * The state will be reflected when a publisher received is sent to subscriber
     * </p>
     * <p>
     * <b>Note : </b> This ensures that a given message has successfully being received by the subscriber
     * </p>
     *
     * @param messageId the unique message id
     */
    public void messageAcknowledged(int messageId) {
        acknowledgedMessages.add(messageId);
    }

    /**
     * <p>
     * Ensures whether this message has already being acknowledged
     * </p>
     *
     * @param messageId the id of the message
     * @return true if the message has already being acknowledged
     */
    public boolean hasAcknowledged(int messageId) {
        return acknowledgedMessages.contains(messageId);
    }

    /**
     * <p>
     * invoke when the message delivery is completed, this will clear up state
     * </p>
     *
     * @param messageId the id of the message
     */
    public void messageDeliveryCompleted(int messageId) {
        acknowledgedMessages.remove(messageId);
    }


}
