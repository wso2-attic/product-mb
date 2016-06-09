/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.carbon.andes.transports.mqtt.adaptors.andes.subscriptions;

import org.wso2.carbon.andes.core.subscription.TopicSubscriptionBitMapStore;
import org.wso2.carbon.andes.transports.mqtt.adaptors.andes.utils.MqttUtils;

/**
 * The bitmap subscription store implementation of the MQTT protocol.
 */
public class MQTTopicSubscriptionBitMapStore extends TopicSubscriptionBitMapStore {

    /**
     * Initialize the bitmap store with MQTT specific properties.
     */
    public MQTTopicSubscriptionBitMapStore() {
        super(MqttUtils.MULTI_LEVEL_WILDCARD, MqttUtils.SINGLE_LEVEL_WILDCARD, "/");
    }

    /**
     * Check if a given wildcard matches with a given none wildcard destination according to MQTT spec.
     *
     * @param wildcardDestination    The wildcard destination
     * @param nonWildcardDestination The non wildcard destination to check against
     * @return True if non wildcard destination is implied by the wildcard
     */
    @Override
    protected boolean isMatchForProtocolType(String wildcardDestination, String nonWildcardDestination) {
        return MqttUtils.isTargetQueueBoundByMatchingToRoutingKey(wildcardDestination, nonWildcardDestination);
    }
}
