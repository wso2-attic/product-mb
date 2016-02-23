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

package org.wso2.carbon.andes.transports.mqtt.distribution.bridge;

import org.wso2.andes.kernel.DeliverableAndesMetadata;

/**
 * Contains the delivery tag which will be generated to sent to subscribers
 */
public class MessageDeliveryTag implements Comparable<MessageDeliveryTag> {

    /**
     * Holds the message id which will be sent to the client
     */
    private int messageId;

    /**
     * Indicates the number of times the keep alive time has traversed through this object
     * This will be used to identify whether the messages should be re-sent
     */
    private int keepAliveTraversal = 0;

    /**
     * Holds the meta data of the message the delivery tag is generated for
     * This data is required to be submitted to Andes if the message remains un-acked
     */
    private DeliverableAndesMetadata messageMetaInformation;

    public void incrementTraversalCount() {
        this.keepAliveTraversal = this.keepAliveTraversal + 1;
    }

    public int getMessageId() {
        return messageId;
    }

    public int getKeepAliveTraversal() {
        return keepAliveTraversal;
    }

    public MessageDeliveryTag(int mid) {
        this.messageId = mid;

    }

    public DeliverableAndesMetadata getMessageMetaInformation() {
        return messageMetaInformation;
    }

    public void setMessageMetaInformation(DeliverableAndesMetadata messageMetaInformation) {
        this.messageMetaInformation = messageMetaInformation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MessageDeliveryTag that = (MessageDeliveryTag) o;

        return messageId == that.messageId;

    }

    @Override
    public int hashCode() {
        return Integer.hashCode(messageId);
    }

    @Override
    public int compareTo(MessageDeliveryTag deliveryTag) {
        int result;

        if (this.getKeepAliveTraversal() > deliveryTag.getKeepAliveTraversal()) {
            result = -1;
        } else if (this.getKeepAliveTraversal() < deliveryTag.getKeepAliveTraversal()) {
            result = 1;
        } else {
            result = 0;
        }

        return result;
    }
}
