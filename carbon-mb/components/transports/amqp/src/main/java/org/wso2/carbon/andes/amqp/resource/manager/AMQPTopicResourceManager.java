/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.carbon.andes.amqp.resource.manager;

import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.server.resource.manager.DefaultResourceHandler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AMQP resource handler for topics.
 */
public class AMQPTopicResourceManager extends DefaultResourceHandler {
    /**
     * Wildcard character to include all.
     */
    private static final String ALL_WILDCARD = "*";
    private ProtocolType protocolType;
    private DestinationType destinationType;

    public AMQPTopicResourceManager(ProtocolType protocolType, DestinationType destinationType) {
        super(protocolType, destinationType);
        this.protocolType = protocolType;
        this.destinationType = destinationType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDestinations() throws AndesException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesQueue createDestination(String s, String s1) throws AndesException {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDestination(String s) throws AndesException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(String s) throws AndesException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesSubscription> getSubscriptions(String subscriptionName, String destinationName, boolean active,
                                                    int offset, int limit) throws AndesException {
        Set<AndesSubscription> allClusterSubscriptions = AndesContext.getInstance()
                .getSubscriptionEngine().getAllClusterSubscriptionsForDestinationType(protocolType, destinationType);

        Set<AndesSubscription> filteredSubscriptions = allClusterSubscriptions.stream()
                .filter(s -> s.getProtocolType() == protocolType)
                .filter(s -> s.isDurable() == ((destinationType == DestinationType.QUEUE)
                                               || (destinationType == DestinationType.DURABLE_TOPIC)))
                .filter(s -> s.hasExternalSubscriptions() == active)
                .filter(s -> null != subscriptionName
                             && !ALL_WILDCARD.equals(subscriptionName)
                             && s.getSubscriptionID().contains(subscriptionName))
                .filter(s -> null != destinationName
                             && !ALL_WILDCARD.equals(destinationName)
                             && s.getSubscribedDestination().equals(destinationName))
                .collect(Collectors.toSet());

        filteredSubscriptions = filterTopicSubscriptions(filteredSubscriptions);

        return filteredSubscriptions
                .stream()
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Filters out topic subscriptions to support shared subscriptions.
     *
     * @param subscriptions A set of {@link AndesSubscription}.
     * @return Filtered out set of {@link AndesSubscription}.
     */
    private Set<AndesSubscription> filterTopicSubscriptions(Set<AndesSubscription> subscriptions) {
        Set<AndesSubscription> subscriptionsToDisplay = new HashSet<>();

        Map<String, AndesSubscription> inactiveSubscriptions = new HashMap<>();
        Set<String> uniqueSubscriptionIDs = new HashSet<>();

        for (AndesSubscription subscription : subscriptions) {

            if (subscription.isDurable()) {
                if (subscription.hasExternalSubscriptions()) {
                    uniqueSubscriptionIDs.add(subscription.getTargetQueue());
                } else {
                    // Since only one inactive shared subscription should be shown
                    // we replace the existing value if any
                    inactiveSubscriptions.put(subscription.getTargetQueue(), subscription);
                    // Inactive subscriptions will be added later considering shared subscriptions
                    continue;
                }
            }

            subscriptionsToDisplay.add(subscription);
        }

        // In UI only one inactive shared subscription should be shown if there are no active subscriptions.
        // If there are active subscriptions with same target queue, we skip adding inactive subscriptions
        subscriptionsToDisplay.addAll(inactiveSubscriptions.entrySet()
                .stream()
                .filter(inactiveEntry -> !(uniqueSubscriptionIDs.contains(inactiveEntry.getKey())))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList()));

        return subscriptionsToDisplay;
    }
}
