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

package org.wso2.carbon.andes.amqp.subscription;

import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.subscription.TopicSubscriptionBitMapStore;

/**
 * AMQP specific Subscription store implementation which stores AMQP topic subscriptions.
 * This resolves AMQP specific wildcards.
 */
public class AMQPTopicSubscriptionBitMapStore extends TopicSubscriptionBitMapStore {

    /**
     * Initialize the bitmap store with AMQP specific properties.
     */
    public AMQPTopicSubscriptionBitMapStore() {
        // AMQPUtils keep wildcard concatenated with constituent delimiter, hence removing them to get wildcard only
        super(AMQPUtils.TOPIC_AND_CHILDREN_WILDCARD.replace(".", ""),
                AMQPUtils.IMMEDIATE_CHILDREN_WILDCARD.replace(".", ""),
                ".");

    }

    /**
     * Check if a given wildcard matches with a given none wildcard destination according to AMQPSpec.
     *
     * @param wildcardDestination The wildcard destination
     * @param nonWildcardDestination The non wildcard destination to check against
     *
     * @return True if non wildcard destination is implied by the wildcard
     */
    @Override
    protected boolean isMatchForProtocolType(String wildcardDestination, String nonWildcardDestination) {
        return AMQPUtils.isTargetQueueBoundByMatchingToRoutingKey(wildcardDestination, nonWildcardDestination);
    }
}
