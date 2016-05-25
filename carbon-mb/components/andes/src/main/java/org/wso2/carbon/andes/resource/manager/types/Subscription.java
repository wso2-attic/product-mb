package org.wso2.carbon.andes.resource.manager.types;

import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;

/**
 *
 */
public class Subscription {
    private String subscriptionIdentifier;
    private String subscribedDestinationName;
    private String subscriberQueueBoundExchange;
    private String subscriberQueueName;
    private String destination;
    private boolean isDurable;
    private boolean isActive;
    private long numberOfMessagesRemainingForSubscriber;
    private String subscriberNodeAddress;
    private ProtocolType protocolType;
    private DestinationType destinationType;

    public String getSubscriptionIdentifier() {
        return subscriptionIdentifier;
    }

    public void setSubscriptionIdentifier(String subscriptionIdentifier) {
        this.subscriptionIdentifier = subscriptionIdentifier;
    }

    public String getSubscriberNodeAddress() {
        return subscriberNodeAddress;
    }

    public void setSubscriberNodeAddress(String subscriberNodeAddress) {
        this.subscriberNodeAddress = subscriberNodeAddress;
    }

    public String getSubscribedDestinationName() {
        return subscribedDestinationName;
    }

    public void setSubscribedDestinationName(String subscribedDestinationName) {
        this.subscribedDestinationName = subscribedDestinationName;
    }

    public String getSubscriberQueueName() {
        return subscriberQueueName;
    }

    public void setSubscriberQueueName(String subscriberQueueName) {
        this.subscriberQueueName = subscriberQueueName;
    }

    public boolean isDurable() {
        return isDurable;
    }

    public void setDurable(boolean durable) {
        isDurable = durable;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public long getNumberOfMessagesRemainingForSubscriber() {
        return numberOfMessagesRemainingForSubscriber;
    }

    public void setNumberOfMessagesRemainingForSubscriber(long numberOfMessagesRemainingForSubscriber) {
        this.numberOfMessagesRemainingForSubscriber = numberOfMessagesRemainingForSubscriber;
    }

    public String getSubscriberQueueBoundExchange() {
        return subscriberQueueBoundExchange;
    }

    public void setSubscriberQueueBoundExchange(String subscriberQueueBoundExchange) {
        this.subscriberQueueBoundExchange = subscriberQueueBoundExchange;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public ProtocolType getProtocolType() {
        return protocolType;
    }

    public void setProtocolType(ProtocolType protocolType) {
        this.protocolType = protocolType;
    }

    public DestinationType getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(DestinationType destinationType) {
        this.destinationType = destinationType;
    }
}
