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

package org.wso2.mb.migration;

import java.util.HashMap;
import java.util.Map;

public class Modifier {

    /**
     * Properties of a subscription.
     */
    private Map<String, String> subscriptionProperties;

    /**
     * The storage queue of the last read subscription.
     */
    private String storageQueueName;

    /**
     * Method to modify a subscription.
     * <p/>
     * Removes the subscriptionType and isBoundToTopic properties and inserts destination type and the protocol type
     *
     * @param oldSubscription subscription String to be modified
     * @return String representing the modified subscription
     */
    public String modifySubscription(String oldSubscription) {
        String newSubscription;

        String[] subscriptionProperties = oldSubscription.split(",");

        this.subscriptionProperties = new HashMap<>();

        for (String entry : subscriptionProperties){
            if (entry.trim().length() > 0) {
                entry = entry.trim();
                String[] keyValuePairs = entry.split("=");
                this.subscriptionProperties.put(keyValuePairs[0], keyValuePairs[1]);
            }

        }
        storageQueueName = this.subscriptionProperties.get("storageQueueName");
        newSubscription = encodeAsStr();
        return newSubscription;
    }

    /**
     * Method to derive the new destination type given the old destination type.
     * <p/>
     * E.g., topic.topic1 is modified to DURABLE_TOPIC.topic1
     *
     * @param oldDest destination type to be modifed
     * @return destination after modification
     */
    public String modifyDestinationType(String oldDest){
        String newDest = oldDest;
        if (oldDest.startsWith("topic.")){
            newDest=oldDest.substring(6, oldDest.length());
            newDest= "DURABLE_TOPIC." + newDest;
        }
        return newDest;
    }

    /**
     * The destination type.
     *
     * Indicates if it is bound to queue or topic and the durability
     */
    private String destinationType = "";

    /**
     * Indicates the protocol type of the queue, binding,, subscription, etc. Will be either "AMQP" or "MQTT"
     */
    private String protocolType = "";

    /**
     * Create new string representing the subscription using the subscription properties
     *
     * @return String representing the new subscriptions
     */
    private String encodeAsStr() {

        if ("true".equals(subscriptionProperties.get("isBoundToTopic"))) {
            if ("true".equals(subscriptionProperties.get("isDurable"))) {
                destinationType = "DURABLE_TOPIC";
            } else {
                destinationType = "TOPIC";
            }
        } else {
            destinationType = "QUEUE";
        }

        if ("AMQP".equals(subscriptionProperties.get("subscriptionType"))) {
            protocolType = "AMQP";

        } else {
            protocolType = "MQTT";
        }

        // Append all the properties to get the subscription details
        StringBuilder builder = new StringBuilder();
        builder.append("subscriptionID=").append(subscriptionProperties.get("subscriptionID"))
                .append(",destination=").append(subscriptionProperties.get("destination"))
                .append(",isExclusive=").append(subscriptionProperties.get("isExclusive"))
                .append(",isDurable=").append(subscriptionProperties.get("isDurable"))
                .append(",targetQueue=").append(subscriptionProperties.get("targetQueue"))
                .append(",targetQueueOwner=")
                .append(subscriptionProperties.get("targetQueueOwner"))
                .append(",targetQueueBoundExchange=")
                .append(subscriptionProperties.get("targetQueueBoundExchange"))
                .append(",targetQueueBoundExchangeType=")
                .append(subscriptionProperties.get("targetQueueBoundExchangeType"))
                .append(",isTargetQueueBoundExchangeAutoDeletable=")
                .append(subscriptionProperties.get("isTargetQueueBoundExchangeAutoDeletable"))
                .append(",subscribedNode=").append(subscriptionProperties.get("subscribedNode"))
                .append(",subscribedTime=").append(subscriptionProperties.get("subscribedTime"))
                .append(",hasExternalSubscriptions=").append(subscriptionProperties.get("hasExternalSubscriptions"))
                .append(",storageQueueName=").append(subscriptionProperties.get("storageQueueName"))
                .append(",destinationType=").append(destinationType)
                .append(",protocolType=").append(protocolType);

        return builder.toString();
    }

    /**
     * Modify queue info to be compatible with MB 3.1.0. Adds the protocol type and the destination type.
     *
     * @param queueInfo queue information to be modified
     * @return modified queue info
     */
    public String modifyQueue(String queueInfo){
        if (null != queueInfo) {
            if ("MQTT".equals(protocolType)){
                destinationType = "TOPIC";
            }
            else{
                destinationType = "QUEUE";
            }
            StringBuilder builder = new StringBuilder();
            builder.append(queueInfo).append(",protocolType=").append(protocolType).append(",destinationType=").append(destinationType);

            return builder.toString();
        }
        else {
            throw new RuntimeException("Queue info cannot be null");
        }
    }

    /**
     * Modify queue info to be compatible with MB 3.1.0. Adds the protocol type and the destination type.
     *
     * @param queueInfo string representing the queue details to be modified
     * @return String representing modified queue details
     */
    public String modifyDefaultQueue(String queueInfo){
        if (null != queueInfo) {
            StringBuilder builder = new StringBuilder();
            builder.append(queueInfo).append(",protocolType=").append("AMQP").append(",destinationType=").append("QUEUE");
            return builder.toString();
        }
        else {
            throw new RuntimeException("Queue info cannot be empty");
        }
    }

    /**
     * Modify binding info to be compatible with MB 3.1.0. Adds the protocol type and the destination type.
     *
     * @param bindingInfo binding information to be modified
     * @return modified queue info
     */
    public String modifyBinding(String bindingInfo){

        if (null != bindingInfo) {
            String[] parts = bindingInfo.split("\\|");
            StringBuilder builder = new StringBuilder();
            builder.append(parts[0].trim())
                    .append("|")
                    .append(parts[1].trim())
                    .append(",protocolType=")
                    .append(protocolType)
                    .append(",destinationType=QUEUE")
                    .append("|")
                    .append(parts[2]);

            return builder.toString();
        }
        else{
            throw new RuntimeException("Binding cannot be null");
        }
    }

    /**
     * Method to modify bindings that have no subscriptions
     *
     * @param bindingInfo binding information to be modified
     * @return String representing binding information after modification
     */
    public String modifyDefaultBinding(String bindingInfo){

        if (null != bindingInfo) {
            String[] parts = bindingInfo.split("\\|");
            StringBuilder builder = new StringBuilder();
            builder.append(parts[0].trim())
                    .append("|")
                    .append(parts[1].trim())
                    .append(",protocolType=")
                    .append("AMQP")
                    .append(",destinationType=QUEUE")
                    .append("|")
                    .append(parts[2]);

            return builder.toString();
        }
        else{
            throw new RuntimeException("Binding cannot be null");
        }
    }

    public String getStorageQueueName() {
        return storageQueueName;
    }
}
