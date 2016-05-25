package org.wso2.carbon.andes.resource.manager.utils;

import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.carbon.andes.resource.manager.types.Subscription;

/**
 *
 */
public class SubscriptionConverterHelper {

    public Subscription getAndesSubscriptionAsSubscription(Andes andesInstance, ProtocolType protocolType,
                                                            DestinationType destinationType, AndesSubscription
                                                                    andesSubscription) throws AndesException {
        Subscription subscription = new Subscription();
        subscription.setSubscriptionIdentifier(andesSubscription.getSubscriptionID());
        subscription.setSubscribedDestinationName(andesSubscription.getSubscribedDestination());
        subscription.setSubscriberQueueBoundExchange(andesSubscription.getTargetQueueBoundExchangeName());
        subscription.setSubscriberQueueName(andesSubscription.getTargetQueue());
        subscription.setDurable(andesSubscription.isDurable());
        subscription.setActive(andesSubscription.hasExternalSubscriptions());
        subscription.setNumberOfMessagesRemainingForSubscriber(andesInstance.getMessagingEngine()
                .getMessageCountOfQueue(andesSubscription.getStorageQueueName()));
        subscription.setSubscriberNodeAddress(andesSubscription.getSubscribedNode());
        subscription.setProtocolType(protocolType);
        subscription.setDestinationType(destinationType);
        return subscription;
    }
}
