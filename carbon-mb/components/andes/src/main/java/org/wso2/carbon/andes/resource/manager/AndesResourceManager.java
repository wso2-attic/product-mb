package org.wso2.carbon.andes.resource.manager;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.carbon.andes.resource.manager.types.Destination;
import org.wso2.carbon.andes.resource.manager.types.Message;
import org.wso2.carbon.andes.resource.manager.types.Subscription;

import java.util.List;

/**
 *
 */
public class AndesResourceManager {
    Table<ProtocolType, DestinationType, ResourceManager> resourceManagerTable = HashBasedTable.create();

    /**
     * Gets the collection of destinations(queues/topics)
     *
     * @param protocol        The protocol type matching for the destination type. Example : AMQP, amqp, MQTT, mqtt.
     * @param destinationType The destination type matching for the destination. Example : queue, topic, durable_topic.
     * @param keyword         Search keyword for destination name. "*" will return all destinations. Destinations that
     *                        <strong>contains</strong> the keyword will be returned.
     * @param offset          The offset value for the collection of destination.
     * @param limit           The number of records to return from the collection of destinations.
     * @return A {@link Destination} list with details of destinations.
     */
    public List<Destination> getDestinations(ProtocolType protocol, DestinationType destinationType, String keyword,
                                             int offset, int limit) throws AndesException {
        return resourceManagerTable.get(protocol, destinationType).getDestinations(keyword, offset, limit);
    }

    /**
     * Deletes all the destinations.
     *
     * @param protocol        The protocol type matching for the destination type. Example : amqp, mqtt.
     * @param destinationType The destination type matching for the destination. Example : queue, topic, durable_topic.
     */
    public void deleteDestinations(ProtocolType protocol, DestinationType destinationType) throws AndesException {
        resourceManagerTable.get(protocol, destinationType).deleteDestinations();
    }

    /**
     * Gets a destination.
     *
     * @param protocol        The protocol type matching for the destination type. Example : amqp, mqtt.
     * @param destinationType The destination type matching for the destination. Example : queue, topic, durable_topic.
     * @param destinationName The name of the destination.
     * @return A {@link Destination} with details of the destination.
     */
    public Destination getDestination(ProtocolType protocol, DestinationType destinationType, String destinationName)
                                                                                                throws AndesException {
        return resourceManagerTable.get(protocol, destinationType).getDestination(destinationName);
    }

    /**
     * Creates a new destination.
     *
     * @param protocol        The protocol type matching for the destination type. Example : amqp, mqtt.
     * @param destinationType The destination type matching for the destination. Example : queue, topic, durable_topic.
     * @param destinationName The name of the destination.
     * @param currentUsername The username of the user who creates the destination.
     * @return A {@link Destination} with details of the newly created destination.
     */
    public Destination createDestination(ProtocolType protocol, DestinationType destinationType, String
            destinationName, String currentUsername) throws AndesException {
        return resourceManagerTable.get(protocol, destinationType).createDestination(destinationName, currentUsername);
    }

    /**
     * Deletes a destination.
     *
     * @param protocol        The protocol type matching for the destination type. Example : amqp, mqtt.
     * @param destinationType The destination type matching for the destination. Example : queue, topic, durable_topic.
     * @param destinationName The name of the destination to be deleted.
     */
    public void deleteDestination(ProtocolType protocol, DestinationType destinationType, String destinationName)
                                                                                                throws AndesException {
        resourceManagerTable.get(protocol, destinationType).deleteDestination(destinationName);
    }

    /**
     * Gets subscriptions belonging to a specific protocol type and destination type. The subscriptions can be filtered
     * by subscription name, destination name and whether they are active or not.
     *
     * @param protocol         The protocol type matching for the subscription. Example : amqp, mqtt.
     * @param destinationType  The destination type matching for the subscription. Example : queue, topic,
     *                         durable_topic.
     * @param subscriptionName The name of the subscription. If "*", all subscriptions are included. Else subscriptions
     *                         that <strong>contains</strong> the value are included.
     * @param destinationName  The name of the destination name. If "*", all destinations are included. Else
     *                         destinations that <strong>equals</strong> the value are included.
     * @param active           Filtering the subscriptions that are active or inactive.
     * @param offset           The starting index to return.
     * @param limit            The number of subscriptions to return.
     * @return An list of {@link Subscription} representing subscriptions.
     */
    public List<Subscription> getSubscriptions(ProtocolType protocol, DestinationType destinationType, String
            subscriptionName, String destinationName, boolean active, int offset, int limit) throws AndesException {
        return resourceManagerTable.get(protocol, destinationType).getSubscriptions(subscriptionName,
                destinationName, active, offset, limit);
    }

    /**
     * Close/unsubscribe subscriptions forcefully belonging to a specific protocol type, destination type.
     *
     * @param protocol        The protocol type matching for the subscription. Example : amqp, mqtt.
     * @param destinationType The subscription type matching for the subscription. Example : queue, topic,
     *                        durable_topic.
     * @param destinationName The name of the destination to close/unsubscribe. If "*", all destinations are included.
     *                        Else destinations that <strong>contains</strong> the value are included.
     */
    public void removeSubscriptions(ProtocolType protocol, DestinationType destinationType, String destinationName)
                                                                                                throws AndesException {
        resourceManagerTable.get(protocol, destinationType).removeSubscriptions(destinationName);
    }

    /**
     * Close/Remove/Unsubscribe subscriptions forcefully belonging to a specific protocol type, destination type.
     *
     * @param protocol        The protocol type matching for the subscription. Example : amqp, mqtt.
     * @param destinationType The subscription type matching for the subscription. Example : queue, topic,
     *                        durable_topic.
     * @param destinationName The name of the destination to close/unsubscribe. If "*", all destinations are included.
     *                        Else destinations that <strong>equals</strong> the value are included.
     */
    public void removeSubscription(ProtocolType protocol, DestinationType destinationType, String destinationName,
                                   String subscriptionId) throws AndesException {
        resourceManagerTable.get(protocol, destinationType).removeSubscription(destinationName, subscriptionId);
    }

    /**
     * Browse message of a destination using message ID.
     *
     * @param protocol        The protocol type matching for the message. Example : amqp, mqtt.
     * @param destinationType The destination type matching for the message. Example : queue, topic, durable_topic.
     * @param destinationName The name of the destination
     * @param content         Whether to return message content or not.
     * @param nextMessageID   The starting message ID to return from.
     * @param limit           The number of messages to return.
     * @return An list of {@link Message} representing a collection messages.
     */
    public List<Message> browseDestinationWithMessageID(ProtocolType protocol, DestinationType destinationType,
                                                        String destinationName, boolean content, long nextMessageID,
                                                        int limit) throws AndesException {
        return resourceManagerTable.get(protocol, destinationType).browseDestinationWithMessageID(destinationName,
                content, nextMessageID, limit);
    }

    /**
     * Browse message of a destination. Please note this is time costly.
     *
     * @param protocol        The protocol type matching for the message. Example : amqp, mqtt.
     * @param destinationType The destination type matching for the message. Example : queue, topic, durable_topic.
     * @param destinationName The name of the destination
     * @param content         Whether to return message content or not.
     * @param offset          Starting index of the messages to return.
     * @param limit           The number of messages to return.
     * @return An list of {@link Message} representing a collection messages.
     */
    public List<Message> browseDestinationWithOffset(ProtocolType protocol, DestinationType destinationType, String
            destinationName, boolean content, int offset, int limit) throws AndesException {
        return resourceManagerTable.get(protocol, destinationType).browseDestinationWithOffset(destinationName,
                content, offset, limit);
    }

    /**
     * Gets a message by message ID belonging to a particular protocol, destination type and destination name.
     *
     * @param protocol        The protocol type matching for the message. Example : amqp, mqtt.
     * @param destinationType The destination type matching for the message. Example : queue, topic, durable_topic.
     * @param destinationName The name of the destination to which the message belongs to.
     * @param andesMessageID  The message ID. This message is the andes metadata message ID.
     * @param content         Whether to return content or not.
     * @return A {@link Message} representing a message.
     */
    public Message getMessage(ProtocolType protocol, DestinationType destinationType, String destinationName, long
            andesMessageID, boolean content) throws AndesException {
        return resourceManagerTable.get(protocol, destinationType).getMessage(destinationName, andesMessageID, content);
    }

    /**
     * Purge all messages belonging to a destination.
     *
     * @param protocol        The protocol type matching for the message. Example : amqp, mqtt.
     * @param destinationType The destination type matching for the message. Example : queue, topic, durable_topic.
     * @param destinationName The name of the destination to purge messages.
     */
    public void deleteMessages(ProtocolType protocol, DestinationType destinationType, String destinationName) throws
            AndesException {
        resourceManagerTable.get(protocol, destinationType).deleteMessages(destinationName);
    }

    public void registerResourceManager(ProtocolType protocolType, DestinationType destinationType, ResourceManager
            resourceManagement) {
        resourceManagerTable.put(protocolType, destinationType, resourceManagement);
    }
}
