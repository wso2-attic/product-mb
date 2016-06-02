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

package org.wso2.carbon.transport.tests.mqtt.broker.v311.adaptors;

import org.wso2.carbon.andes.transports.mqtt.adaptors.memory.MemoryConnector;
import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;

import java.util.Map;

/**
 * Mocks the memory adopter for mock testing
 */
public class MockMemoryAdaptor extends MemoryConnector {

    /**
     * Returns a verification on existance of a subscription
     *
     * @param topicName the name of the topic
     * @return true if the subscription exists for the given topic
     */
    public boolean hasSubscriber(String topicName) {
        return subscriptions.get(topicName) != null;
    }

    /**
     * Returns a verification on existence of live subscription for the topic
     * @param topicName the name of the topic
     * @param clientId the id of the client who disconnected from the topic
     * @return true if the subscription is alive
     */
    public boolean isSubscriptionLive(String topicName,String clientId){
        Map<String, MqttChannel> stringMqttChannelMap = subscriptions.get(topicName);
        MqttChannel channel = null;
        if (stringMqttChannelMap != null) {
            channel = stringMqttChannelMap.get(clientId);
        }
        return channel != null;
    }
}
