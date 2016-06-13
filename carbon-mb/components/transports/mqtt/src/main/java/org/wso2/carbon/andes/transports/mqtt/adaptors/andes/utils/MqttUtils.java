/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.carbon.andes.transports.mqtt.adaptors.andes.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This class will contain operations such as conversion of message which are taken from the protocol to be compliant
 * with the objects expected by Andes, message id generation, construction of meta information,
 */
public class MqttUtils {

    private static Log log = LogFactory.getLog(MqttUtils.class);
    public static final String MESSAGE_ID = "MessageID";
    public static final String QOSLEVEL = "QOSLevel";

    public static final String SINGLE_LEVEL_WILDCARD = "+";
    public static final String MULTI_LEVEL_WILDCARD = "#";

    public static final String DEFAULT_ANDES_CHANNEL_IDENTIFIER = "MQTT-Unknown";

    public static final String CLUSTER_SUB_ID_PROPERTY_NAME = "cluster_sub_id";

    /**
     * MQTT Publisher ID
     */
    public static final String CLIENT_ID = "clientID";

    /**
     * Will convert between the types of the QOS to adhere to the conversion of both andes and mqtt protocol
     *
     * @param qos the quality of service level the message should be published/subscribed
     * @return the level which is compliment by the mqtt library
     */
    public static AbstractMessage.QOSType getQOSType(int qos) {
        return AbstractMessage.QOSType.valueOf(qos);
    }


    /**
     * Check if a subscribed queue bound destination routing key matches with a given message routing key using MQTT
     * wildcards.
     *
     * @param queueBoundRoutingKey The subscribed destination with/without wildcards
     * @param messageRoutingKey    The message destination routing key without wildcards
     * @return Is queue bound routing key match the message routing key
     */
    public static boolean isTargetQueueBoundByMatchingToRoutingKey(String queueBoundRoutingKey,
                                                                   String messageRoutingKey) {
        return matchTopics(messageRoutingKey, queueBoundRoutingKey);
    }

    private static boolean matchTopics(String msgTopic, String subscriptionTopic) {

        try {
            List<Token> msgTokens = splitTopic(msgTopic);
            List<Token> subscriptionTokens = splitTopic(subscriptionTopic);
            int i = 0;
            Token subToken = null;
            for (; i < subscriptionTokens.size(); i++) {
                subToken = subscriptionTokens.get(i);
                if (subToken != Token.MULTI && subToken != Token.SINGLE) {
                    if (i >= msgTokens.size()) {
                        return false;
                    }
                    Token msgToken = msgTokens.get(i);
                    if (!msgToken.equals(subToken)) {
                        return false;
                    }
                } else {
                    if (subToken == Token.MULTI) {
                        return true;
                    }
                }
            }

            return i == msgTokens.size();
        } catch (ParseException ex) {
            log.error(null, ex);
            throw new RuntimeException(ex);
        }
    }

    private static List<Token> splitTopic(String topic)
            throws ParseException {
        List res = new ArrayList<Token>();
        String[] splitted = topic.split("/");

        if (splitted.length == 0) {
            res.add(Token.EMPTY);
        }

        for (int i = 0; i < splitted.length; i++) {
            String s = splitted[i];
            if (s.isEmpty()) {
                res.add(Token.EMPTY);
            } else if (s.equals("#")) {
                //check that multi is the last symbol
                if (i != splitted.length - 1) {
                    throw new ParseException("Bad format of topic, the multi symbol (#) has to be the last one after "
                            + "a separator", i);
                }
                res.add(Token.MULTI);
            } else if (s.contains("#")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else if (s.equals("+")) {
                res.add(Token.SINGLE);
            } else if (s.contains("+")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else {
                res.add(new Token(s));
            }
        }

        return res;
    }

    /**
     * Checks whether a given subscription is a wildcard subscription.
     *
     * @param subscribedDestination The destination string subscriber subscribed to
     * @return is this a wild card subscription
     */
    public static boolean isWildCardSubscription(String subscribedDestination) {
        boolean isWildCard = false;

        if (subscribedDestination.contains(SINGLE_LEVEL_WILDCARD) || subscribedDestination.contains
                (MULTI_LEVEL_WILDCARD)) {
            isWildCard = true;
        }

        return isWildCard;
    }

    /**
     * Generate a unique UUID for a given client who has subscribed to a given topic with given qos and given clean
     * session.
     *
     * @param clientId     The MQTT client Id
     * @param topic        The topic subscribed to
     * @param qos          The Quality of Service level subscribed to
     * @param cleanSession Clean session value of the client
     * @return A unique UUID for the given arguments
     */
    public static UUID generateSubscriptionChannelID(String clientId, String topic, int qos, boolean cleanSession) {
        return UUID.nameUUIDFromBytes((clientId + topic + qos + cleanSession).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Extract tenant name from a given topic.
     *
     * @param topic The topic to retrieve tenant name
     * @return Tenant name extracted from topic
     */
/*    public static String getTenantFromTopic(String topic) {
     *//*   String tenant = MultitenantConstants.SUPER_TENANT_DOMAIN_NAME;

        if (null != topic && topic.contains(AndesConstants.TENANT_SEPARATOR)) {
            tenant = topic.split(AndesConstants.TENANT_SEPARATOR)[0];
        }

        return tenant;*//*
    }*/
    public static String getTopicSpecificQueueName(String clientId, String topic) {
        return "carbon:" + clientId + ":" + topic;
    }

    /**
     * Given the clean session value and qos level, decide whether it if falling into durable path.
     *
     * @param cleanSession The clean session value
     * @param qos          The quality of service level
     * @return true if this falls into durable path
     */
    public static boolean isDurable(boolean cleanSession, int qos) {
        boolean durable = false;

        if (!cleanSession && qos > 0) {
            durable = true;
        }

        return durable;
    }
}
