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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesUtils;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.SubscriptionAlreadyExistsException;
import org.wso2.andes.kernel.disruptor.inbound.InboundQueueEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundSubscriptionEvent;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.carbon.andes.transports.mqtt.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.connectors.IConnector;
import org.wso2.carbon.andes.transports.mqtt.distribution.MQTTException;
import org.wso2.carbon.andes.transports.mqtt.distribution.MqttMessage;
import org.wso2.carbon.andes.transports.mqtt.distribution.MqttMessageContext;
import org.wso2.carbon.andes.transports.mqtt.distribution.MqttPublisherChannel;
import org.wso2.carbon.andes.transports.mqtt.distribution.MqttUtils;
import org.wso2.carbon.andes.transports.mqtt.distribution.subscriptions.MqttLocalSubscription;
import org.wso2.carbon.andes.transports.mqtt.exceptions.ConnectorException;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.ConnectMessage;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


/**
 * Connects with andes core
 */
public class AndesConnector implements IConnector {

    /**
     * Will maintain the relation between the publisher client identifiers vs the id generated cluster wide
     * Key of the map would be the mqtt specific client id and the value would be the cluster uuid
     */
    private Map<String, MqttPublisherChannel> publisherTopicCorrelate = new HashMap<>();

    /**
     * Will maintain retain message identification (message id + channel id) until ack received
     * by the subscriber.
     * Retain message acks will not handle in andes level.
     */
    private Set<String> retainMessageIdSet = new HashSet<>();


    private static Log log = LogFactory.getLog(AndesConnector.class);


    @Override
    public void storeConnection(ConnectMessage message) throws ConnectorException {
        //Currently andes store does not hold any connection related information
        //throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     * <p>Registers the subscription in the andes store</p>
     */
    @Override
    public void storeSubscriptions(String topic, String clientId, String username, boolean isCleanSession, QOSLevel
            qos, MqttChannel mqttChannel) throws ConnectorException {

        //Will generate a unique id for subscription
        UUID subscriptionChannelID = MqttUtils.generateSubscriptionChannelID(clientId, topic, qos.getValue(),
                isCleanSession);

        try {

            //create a MqttLocalSubscription wrapping underlying channel
            MqttLocalSubscription mqttTopicSubscriber = createSubscription(topic, clientId, qos.getValue(),
                    subscriptionChannelID, true, isCleanSession, mqttChannel);

            if (mqttTopicSubscriber.isDurable()) {
                //We need to create a queue in-order to preserve messages relevant for the durable subscription
                InboundQueueEvent createQueueEvent = new InboundQueueEvent(
                        MqttUtils.getTopicSpecificQueueName(clientId, topic),
                        username, false, true, ProtocolType.MQTT, DestinationType.TOPIC);
                Andes.getInstance().createQueue(createQueueEvent);
            }

            //Will notify the creation of the client connection
            Andes.getInstance().clientConnectionCreated(subscriptionChannelID);

            //Once the connection is created we register subscription
            LocalSubscription localSubscription = createLocalSubscription(mqttTopicSubscriber, topic, clientId);

            //create open subscription event
            InboundSubscriptionEvent openSubscriptionEvent = new InboundSubscriptionEvent(localSubscription);

            //notify subscription create event
            Andes.getInstance().openLocalSubscription(openSubscriptionEvent);

            //We need to register the subscritpion id in the cluster
            mqttChannel.addProperty(MqttUtils.CLUSTER_SUB_ID_PROPERTY_NAME, String.valueOf(subscriptionChannelID));

            if (log.isDebugEnabled()) {
                log.debug("Subscribe registered to the " + topic + " with channel id " + clientId);
            }


        } catch (MQTTException | AndesException | SubscriptionAlreadyExistsException e) {
            final String message = "Error occurred while creating the topic subscription in the kernel";
            log.error(message, e);
            throw new ConnectorException(message, e);
        }

    }

    /**
     * <p>Stores message in the andes store</p>
     * <p><b>Note : </b> the store will ensure that the message will be delivered to the subscriptions</p>
     * {@inheritDoc}
     */
    @Override
    public void storePublishedMessage(MqttMessageContext messageContext) throws ConnectorException {
        if (messageContext.getMessage().hasArray()) {

            MqttPublisherChannel publisher = publisherTopicCorrelate.get(messageContext.getPublisherID());
            if (null == publisher) {
                //We need to create a new publisher
                publisher = new MqttPublisherChannel(messageContext.getChannel());
                publisherTopicCorrelate.put(messageContext.getPublisherID(), publisher);
                //Finally will register the publisher channel for flow controlling

                String andesChannelId = MqttUtils.DEFAULT_ANDES_CHANNEL_IDENTIFIER;
                if (null != messageContext.getChannel()) {
                    andesChannelId = messageContext.getChannel().remoteAddress().toString().substring(1);
                }

                AndesChannel publisherChannel = null;
                try {
                    publisherChannel = Andes.getInstance().createChannel(andesChannelId, publisher);
                } catch (AndesException e) {
                    String error = "Error occurred while initializing the publisher channel " + publisher
                            .getClusterID();
                    log.error(error, e);
                    throw new ConnectorException(error, e);
                }
                //Set channel details
                //Substring to remove leading slash character from remote address
                publisherChannel.setDestination(messageContext.getTopic());
                publisher.setChannel(publisherChannel);
            }

            //Will get the bytes of the message
            byte[] messageData = messageContext.getMessage().array();
            long messageID = 0L; // unique message Id will be generated By Andes.
            //Will start converting the message body
            AndesMessagePart messagePart = MqttUtils.convertToAndesMessage(messageData, messageID);
            //Will Create the Andes Header
            AndesMessageMetadata messageHeader = MqttUtils.convertToAndesHeader(messageID, messageContext.getTopic(),
                    messageContext.getQosLevel().getValue(), messageData.length, messageContext.isRetain(),
                    publisher, messageContext.isCompressed());

            // Add properties to be used for publisher acks
            messageHeader.addProperty(MqttUtils.CLIENT_ID, messageContext.getPublisherID());
            messageHeader.addProperty(MqttUtils.MESSAGE_ID, messageContext.getMqttLocalMessageID());
            messageHeader.addProperty(MqttUtils.QOSLEVEL, messageContext.getQosLevel().getValue());

            // Publish to Andes core
            AndesMessage andesMessage = new MqttMessage(messageHeader);
            andesMessage.addMessagePart(messagePart);
            Andes.getInstance().messageReceived(andesMessage, publisher.getChannel(), messageContext.getPubAckHandler
                    ());
            if (log.isDebugEnabled()) {
                log.debug(" Message added with message id " + messageContext.getMqttLocalMessageID());
            }

        } else {
            throw new ConnectorException("Message content is not backed by an array, or the array is read-only.");
        }

    }

    /**
     * Stores the disconnect message
     * {@inheritDoc}
     */
    @Override
    public void storeDisconnectMessage(String topicName,
                                       String clientId,
                                       boolean isCleanSession,
                                       QOSLevel qosLevel) throws ConnectorException {

        try {
            UUID subscriberChannel = null;

            MqttLocalSubscription mqttTopicSubscriber = createSubscription(topicName, clientId,
                    qosLevel.getValue(), subscriberChannel, isCleanSession, isCleanSession, null);

            //create a close subscription event
            LocalSubscription localSubscription = createLocalSubscription(mqttTopicSubscriber, topicName,
                    clientId);
            localSubscription.setHasExternalSubscriptions(false);
            InboundSubscriptionEvent subscriptionCloseEvent = new InboundSubscriptionEvent(localSubscription);
            Andes.getInstance().closeLocalSubscription(subscriptionCloseEvent);

            //Will indicate the closure of the subscription connection
            Andes.getInstance().clientConnectionClosed(subscriberChannel);

            if (log.isDebugEnabled()) {
                log.debug("Disconnected subscriber from topic " + topicName);
            }

        } catch (AndesException e) {
            final String message = "Error occurred while removing the subscriber ";
            log.error(message, e);
            throw new ConnectorException(message, e);
        } catch (MQTTException e) {
            final String message = "Error occurred while creating mock subscription for deletion ";
            log.error(message, e);
            throw new ConnectorException(message, e);
        }
    }

    /**
     * Unsubscribe message from ande store
     * {@inheritDoc}
     */
    @Override
    public void storeUnsubscribeMessage(String subscribedTopic, String username, String clientId, boolean
            isCleanSession, QOSLevel qosLevel) throws ConnectorException {
        try {

            UUID subscriberChannel = null;

            String queueIdentifier = MqttUtils.getTopicSpecificQueueName(clientId, subscribedTopic);

            MqttLocalSubscription mqttTopicSubscriber = createSubscription(subscribedTopic,
                    clientId, qosLevel.getValue(), subscriberChannel, false, isCleanSession, null);

            if (mqttTopicSubscriber.isDurable()) {

                //This will be similar to a durable subscription of AMQP
                //There could be two types of events one is the disconnection due to the lost of the connection
                //The other is un-subscription, if is the case of un-subscription the subscription should be removed
                //Andes will automatically remove all the subscriptions bound to a queue when the queue is deleted
                InboundQueueEvent queueChange = new InboundQueueEvent(queueIdentifier, username, false, true,
                        ProtocolType.MQTT, DestinationType.DURABLE_TOPIC);
                Andes.getInstance().deleteQueue(queueChange);
            } else {
                //create a close subscription event
                LocalSubscription localSubscription = createLocalSubscription(mqttTopicSubscriber, subscribedTopic,
                        clientId);
                localSubscription.setHasExternalSubscriptions(false);
                InboundSubscriptionEvent subscriptionCloseEvent = new InboundSubscriptionEvent(localSubscription);
                Andes.getInstance().closeLocalSubscription(subscriptionCloseEvent);

                //Will indicate the closure of the subscription connection
                Andes.getInstance().clientConnectionClosed(subscriberChannel);
            }

            if (log.isDebugEnabled()) {
                log.debug("Disconnected subscriber from topic " + subscribedTopic);
            }

        } catch (AndesException e) {
            final String message = "Error occurred while removing the subscriber ";
            log.error(message, e);
            throw new ConnectorException(message, e);
        } catch (MQTTException e) {
            final String message = "Error occurred while creating mock subscription for removal ";
            log.error(message, e);
            throw new ConnectorException(message, e);
        }

    }

    /**
     * Notifies the message store upon receiving a publisher received ack for QoS 2 message
     * {@inheritDoc}
     */
    @Override
    public void storeSubscriberAcknowledgment(long messageID, MqttChannel channel) throws ConnectorException {

        UUID channelID = UUID.fromString(channel.getProperty(MqttUtils.CLUSTER_SUB_ID_PROPERTY_NAME));
        AndesAckData andesAckData = null;

        try {
            andesAckData = AndesUtils.generateAndesAckMessage(channelID, messageID);
            // Remove retain message ack upon receive from retain message metadata map
            if (retainMessageIdSet.contains(messageID + channelID.toString())) {
                retainMessageIdSet.remove(messageID + channelID.toString());
            } else {
                Andes.getInstance().ackReceived(andesAckData);
            }
        } catch (AndesException e) {
            String error = "Error occurred while processing the subscriber acknowledgment";
            throw new ConnectorException(error, e);
        }

    }

    /**
     * Processors from a periodic task or through a ping request
     * {@inheritDoc}
     */
    @Override
    public void storeRejection(MessageDeliveryTag deliveryTag, MqttChannel channel) throws ConnectorException {
        UUID channelID = UUID.fromString(channel.getProperty(MqttUtils.CLUSTER_SUB_ID_PROPERTY_NAME));
        DeliverableAndesMetadata metadata = deliveryTag.getMessageMetaInformation();
        try {
            Andes.getInstance().messageRejected(metadata, channelID);
        } catch (AndesException e) {
            String error = "Error occurred while sending the rejection";
            throw new ConnectorException(error, e);
        }
    }

    /**
     * Generate a local subscription object using MQTT subscription information
     *
     * @param mqttLocalSubscription instance of underlying mqtt local subscriber
     * @param topic                 subscribed topic name
     * @param clientID              valid only when isCleanSession = false. A unique id should be given
     * @return Local subscription object representing a subscription in Andes kernel
     */
    private LocalSubscription createLocalSubscription(MqttLocalSubscription mqttLocalSubscription, String topic,
                                                      String clientID) {

        boolean isDurable = mqttLocalSubscription.isDurable();
        String subscribedNode = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        long subscribedTime = System.currentTimeMillis();
        String targetQueue;
        String targetQueueOwner = "";
        String targetQueueBoundExchange;
        String targetQueueBoundExchangeType = "";
        Short isTargetQueueBoundAutoDeletable;
        boolean hasExternalSubscriptions = true;
        String queueIdentifier = MqttUtils.getTopicSpecificQueueName(clientID, topic);

        DestinationType destinationType;

        if (isDurable) {
            //For subscription that are durable we need to provide the queue name for the queue identifier
            targetQueue = queueIdentifier;
            targetQueueBoundExchange = AMQPUtils.DIRECT_EXCHANGE_NAME;
            isTargetQueueBoundAutoDeletable = 0;
            destinationType = DestinationType.DURABLE_TOPIC;
        } else {
            //create a andes core LocalSubscription without giving queue names
            targetQueue = topic;
            targetQueueBoundExchange = AMQPUtils.TOPIC_EXCHANGE_NAME;
            isTargetQueueBoundAutoDeletable = 1;
            destinationType = DestinationType.TOPIC;
        }

        LocalSubscription localSubscription = AndesUtils.createLocalSubscription(mqttLocalSubscription, queueIdentifier,
                topic, true, isDurable, subscribedNode, subscribedTime, targetQueue, targetQueueOwner,
                targetQueueBoundExchange, targetQueueBoundExchangeType, isTargetQueueBoundAutoDeletable,
                hasExternalSubscriptions, destinationType);

        return localSubscription;
    }

    /**
     * Will create subscriptions out of the provided list of information, this will be used when creating durable,
     * non durable subscriptions. As well as creating the subscription object for removal
     *
     * @param mqttClientID          the id of the client which is provided by the protocol
     * @param qos                   the level in which the messages would be exchanged this will be either 0,1 or 2
     * @param subscriptionChannelID the id of the channel that would be unique across the cluster
     * @param isActive              is the subscription active it will be inactive during removal
     * @param cleanSession          has the subscriber subscribed with clean session
     * @return the andes specific object that will be registered in the cluster
     * @throws MQTTException
     */
    private MqttLocalSubscription createSubscription(String wildcardDestination,
                                                     String mqttClientID, int qos,
                                                     UUID subscriptionChannelID,
                                                     boolean isActive,
                                                     boolean cleanSession,
                                                     MqttChannel mqttChannel)
            throws MQTTException {

        boolean durable = MqttUtils.isDurable(cleanSession, qos);

        MqttLocalSubscription outBoundTopicSubscription = new MqttLocalSubscription
                (wildcardDestination, subscriptionChannelID, isActive, durable, mqttChannel);

        //  outBoundTopicSubscription.setMqqtServerChannel(channel);
        outBoundTopicSubscription.setMqttSubscriptionID(mqttClientID);
        outBoundTopicSubscription.setSubscriberQOS(qos);

        return outBoundTopicSubscription;

    }


}
