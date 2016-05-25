package org.wso2.carbon.andes.amqp.resource.manager;

import org.apache.commons.lang.NotImplementedException;
import org.wso2.andes.framing.ProtocolVersion;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.carbon.andes.amqp.internal.AMQPComponentDataHolder;
import org.wso2.carbon.andes.amqp.resource.manager.utils.AMQPMessageConverterHelper;
import org.wso2.carbon.andes.resource.manager.ResourceManager;
import org.wso2.carbon.andes.resource.manager.types.Destination;
import org.wso2.carbon.andes.resource.manager.types.Message;
import org.wso2.carbon.andes.resource.manager.types.Subscription;
import org.wso2.carbon.andes.resource.manager.utils.MessageConverterHelper;
import org.wso2.carbon.andes.resource.manager.utils.SubscriptionConverterHelper;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 *
 */
public class AMQPQueueResourceManager extends ResourceManager {
    private Andes andesInstance = AMQPComponentDataHolder.getInstance().getAndesInstance();
    private ProtocolType protocolType;
    private DestinationType destinationType = DestinationType.QUEUE;
    private AMQPMessageConverterHelper amqpMessageConverterHelper;
    private SubscriptionConverterHelper subscriptionConverterHelper;
    private MessageConverterHelper messageConverterHelper;

    /**
     * Wildcard character to include all.
     */
    private static final String ALL_WILDCARD = "*";

    public AMQPQueueResourceManager() throws AndesException {
        protocolType = new ProtocolType("AMQP", ProtocolVersion.v0_91.toString());
        amqpMessageConverterHelper = new AMQPMessageConverterHelper();
        subscriptionConverterHelper = new SubscriptionConverterHelper();
        messageConverterHelper = new MessageConverterHelper();
    }

    @Override
    public List<Destination> getDestinations(String keyword, int offset, int limit) throws AndesException {
        List<Destination> destinations = new ArrayList<>();
        List<AndesQueue> andesQueues = andesInstance.getAMQPConstructStore().getQueues(keyword)
                .stream()
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());

        for (AndesQueue andesQueue : andesQueues) {
            destinations.add(getAndesQueueAsDestination(andesQueue));
        }
        return destinations;
    }

    @Override
    public void deleteDestinations() throws AndesException {
        List<AndesQueue> andesQueues = andesInstance.getAMQPConstructStore().getQueues(ALL_WILDCARD);
        for (AndesQueue andesQueue : andesQueues) {
            deleteDestination(andesQueue.queueName);
        }
    }

    @Override
    public Destination getDestination(String destinationName) throws AndesException {
        AndesQueue queue = andesInstance.getAMQPConstructStore().getQueue(destinationName);
        if (null != queue) {
            return getAndesQueueAsDestination(queue);
        } else {
            return null;
        }
    }

    @Override
    public Destination createDestination(String destinationName, String currentUsername) throws AndesException {
        Destination newDestination;
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("org.wso2.andes:type=VirtualHost.VirtualHostManager," +
                                                   "VirtualHost=\"carbon\"");
            String operationName = "createNewQueue";

            Object[] parameters = new Object[]{destinationName, currentUsername, true};
            String[] signature = new String[]{String.class.getName(), String.class.getName(), boolean.class.getName()};

            mBeanServer.invoke(objectName, operationName, parameters, signature);

            ObjectName bindingMBeanObjectName = new ObjectName("org.wso2.andes:type=VirtualHost.Exchange," +
                                                               "VirtualHost=\"carbon\",name=\"amq.direct\"," +
                                                               "ExchangeType=direct");
            String bindingOperationName = "createNewBinding";

            Object[] bindingParams = new Object[]{destinationName, currentUsername};
            String[] bpSignatures = new String[]{String.class.getName(), String.class.getName()};

            mBeanServer.invoke(bindingMBeanObjectName, bindingOperationName, bindingParams, bpSignatures);

            AndesQueue queue = andesInstance.getAMQPConstructStore().getQueue(destinationName);
            newDestination = getAndesQueueAsDestination(queue);
        } catch (MalformedObjectNameException | InstanceNotFoundException | ReflectionException | MBeanException e) {
            throw new AndesException("Error occurred while creating queue : \"" + destinationName + "\".", e);
        }
        return newDestination;
    }

    @Override
    public void deleteDestination(String destinationName) throws AndesException {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

            ObjectName objectName = new ObjectName("org.wso2.andes:type=VirtualHost.VirtualHostManager," +
                                                   "VirtualHost=\"carbon\"");
            String operationName = "deleteQueue";

            Object[] parameters = new Object[]{destinationName};
            String[] signature = new String[]{String.class.getName()};

            mBeanServer.invoke(objectName, operationName, parameters, signature);
        } catch (MalformedObjectNameException | InstanceNotFoundException | ReflectionException | MBeanException e) {
            throw new AndesException("Error occurred while deleting queue : \"" + destinationName + "\".", e);
        }
    }

    @Override
    public List<Subscription> getSubscriptions(String subscriptionName, String destinationName, boolean active, int
            offset, int limit) throws AndesException {
        List<Subscription> subscriptions = new ArrayList<>();

        Set<AndesSubscription> andesSubscriptions = andesInstance.getSubscriptionEngine()
                .getAllClusterSubscriptionsForDestinationType(protocolType, DestinationType.QUEUE);

        List<AndesSubscription> filteredSubscriptions = andesSubscriptions
                .stream()
                .filter(s -> s.getProtocolType() == protocolType)
                .filter(s -> s.getDestinationType() == DestinationType.QUEUE)
                .filter(s -> s.hasExternalSubscriptions() == active)
                .filter(s -> null != subscriptionName
                             && !ALL_WILDCARD.equals(subscriptionName)
                             && s.getSubscriptionID().contains(subscriptionName))
                .filter(s -> null != destinationName
                             && !ALL_WILDCARD.equals(destinationName)
                             && s.getSubscribedDestination().equals(destinationName))
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());

        for (AndesSubscription subscription : filteredSubscriptions) {
            subscriptions.add(subscriptionConverterHelper.getAndesSubscriptionAsSubscription(andesInstance,
                    protocolType, destinationType, subscription));
        }

        return subscriptions;
    }

    @Override
    public void removeSubscriptions(String destinationName) throws AndesException {
        Set<AndesSubscription> activeLocalSubscribersForNode = andesInstance.getSubscriptionEngine()
                .getActiveLocalSubscribersForNode();

        List<LocalSubscription> subscriptions = activeLocalSubscribersForNode
                .stream()
                .filter(s -> s.getProtocolType() == protocolType)
                .filter(s -> s.getDestinationType() == DestinationType.QUEUE)
                .filter(s -> null != destinationName
                             && !ALL_WILDCARD.equals(destinationName)
                             && s.getSubscribedDestination().contains(destinationName))
                .map(s -> (LocalSubscription) s)
                .collect(Collectors.toList());
        for (LocalSubscription subscription : subscriptions) {
            subscription.forcefullyDisconnect();
        }
    }

    @Override
    public void removeSubscription(String destinationName, String subscriptionId) throws AndesException {
        Set<LocalSubscription> allSubscribersForDestination = andesInstance.getSubscriptionEngine()
                .getActiveLocalSubscribers(destinationName, protocolType, DestinationType.QUEUE);

        LocalSubscription localSubscription = allSubscribersForDestination
                .stream()
                .filter(s -> s.getSubscriptionID().equals(subscriptionId))
                .findFirst().orElse(null);
        if (null != localSubscription) {
            localSubscription.forcefullyDisconnect();
        } else {
            throw new AndesException("Subscription could not be found to disconnect.");
        }
    }

    @Override
    public List<Message> browseDestinationWithMessageID(String destinationName, boolean content, long nextMessageID,
                                                        int limit) throws AndesException {
        List<Message> messages = new ArrayList<>();
        List<AndesMessageMetadata> nextNMessageMetadataFromQueue;
        if (!DLCQueueUtils.isDeadLetterQueue(destinationName)) {
            nextNMessageMetadataFromQueue = Andes.getInstance().getNextNMessageMetadataFromQueue(destinationName,
                    nextMessageID, limit);
        } else {
            nextNMessageMetadataFromQueue = Andes.getInstance().getNextNMessageMetadataFromDLC(destinationName, 0,
                    limit);
        }

        for (AndesMessageMetadata andesMessageMetadata : nextNMessageMetadataFromQueue) {
            Map<String, String> jmsMessageProperties = amqpMessageConverterHelper.getJMSMessageProperties
                    (andesMessageMetadata);
            String jmsMessageContent = amqpMessageConverterHelper.getJMSMessageContent(andesMessageMetadata);
            messages.add(messageConverterHelper.getAndesMessageMetadataAsMessage(protocolType, destinationType,
                    andesMessageMetadata, jmsMessageProperties, jmsMessageContent));
        }

        return messages;
    }

    @Override
    public List<Message> browseDestinationWithOffset(String destinationName, boolean content, int offset, int limit)
            throws AndesException {
        List<Message> messages = new ArrayList<>();

        List<AndesMessageMetadata> nextNMessageMetadataFromQueue;
        if (!DLCQueueUtils.isDeadLetterQueue(destinationName)) {
            nextNMessageMetadataFromQueue = Andes.getInstance().getNextNMessageMetadataFromQueue(destinationName,
                    offset, limit);
        } else {
            nextNMessageMetadataFromQueue = Andes.getInstance().getNextNMessageMetadataFromDLC(destinationName, 0,
                    limit);
        }

        for (AndesMessageMetadata andesMessageMetadata : nextNMessageMetadataFromQueue) {
            Map<String, String> jmsMessageProperties = amqpMessageConverterHelper.getJMSMessageProperties
                    (andesMessageMetadata);
            String jmsMessageContent = amqpMessageConverterHelper.getJMSMessageContent(andesMessageMetadata);
            messages.add(messageConverterHelper.getAndesMessageMetadataAsMessage(protocolType, destinationType,
                    andesMessageMetadata, jmsMessageProperties, jmsMessageContent));
        }
        return messages;
    }

    @Override
    public Message getMessage(String destinationName, long andesMessageID, boolean content) throws AndesException {
        AndesMessageMetadata messageMetadata;
        if (!DLCQueueUtils.isDeadLetterQueue(destinationName)) {
            messageMetadata = Andes.getInstance().getNextNMessageMetadataFromQueue(destinationName, andesMessageID, 1)
                    .stream()
                    .findFirst()
                    .orElse(null);
        } else {
            messageMetadata = Andes.getInstance().getNextNMessageMetadataFromDLC(destinationName, 0, 1)
                    .stream()
                    .findFirst()
                    .orElse(null);
        }

        if (null != messageMetadata) {

            Map<String, String> jmsMessageProperties = amqpMessageConverterHelper.getJMSMessageProperties
                    (messageMetadata);

            String jmsMessageContent = amqpMessageConverterHelper.getJMSMessageContent(messageMetadata);
            return messageConverterHelper.getAndesMessageMetadataAsMessage(protocolType, destinationType,
                    messageMetadata, jmsMessageProperties, jmsMessageContent);
        } else {
            return null;
        }
    }

    @Override
    public void deleteMessages(String destinationName) throws AndesException {
        try {
            throw new NotImplementedException("Deleting messages is not implemented");
        } catch (NotImplementedException e) {
            throw new AndesException("Unable to delete messages of \"" + destinationName + "\".", e);
        }
    }

    private Destination getAndesQueueAsDestination(AndesQueue andesQueue) throws AndesException {
        Destination destination = new Destination();
        destination.setDestinationName(andesQueue.queueName);
        destination.setOwner(andesQueue.queueOwner);
        destination.setDurable(andesQueue.isDurable);
        destination.setSubscriptionCount(andesQueue.subscriptionCount);
        destination.setMessageCount(andesInstance.getMessageCountOfQueue(andesQueue.queueName));
        destination.setProtocol(protocolType);
        destination.setDestinationType(destinationType);
        return destination;
    }
}
