/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.carbon.mb.migration.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueSession;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;
import javax.naming.NamingException;

class JmsTopicSubscriber {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsTopicSubscriber.class);

    private final TopicConnection connection;
    private final TopicSession session;
    private final TopicSubscriber subscriber;
    private final JmsConfig config;
    private int lastReadOffset;

    JmsTopicSubscriber(JmsConfig config) throws JMSException, NamingException {
        InitialContext ctx = config.getInitialContext();
        // Lookup connection factory
        TopicConnectionFactory connFactory =
                (TopicConnectionFactory) ctx.lookup(config.getConnectionFactoryName());
        connection = connFactory.createTopicConnection(config.getUsername(), config.getPassword());
        connection.start();
        session = connection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);

        Topic topic = (Topic) ctx.lookup(config.getDestination());
        if (config.getSubscriptionId().isEmpty()) {
            subscriber = session.createSubscriber(topic);
        } else {
            subscriber = session.createDurableSubscriber(topic, config.getSubscriptionId());
        }
        this.config = config;
        this.lastReadOffset = 0;
    }

    void receive(int timeout, int numberOfMessages) throws JMSException {
        for (int i = 0; i < numberOfMessages; i++) {

            Message message = subscriber.receive(timeout);
            int offset = Integer.parseInt(message.getJMSCorrelationID());
            if (offset > lastReadOffset) {
                lastReadOffset = offset;
            } else {
                LOGGER.error("Message received out of order. Current offset: {}. Last read offset: {}",
                             offset, lastReadOffset);
            }
        }
    }

    void unsubscribe() throws JMSException {
        if (config.getSubscriptionId().isEmpty()) {
            session.unsubscribe(config.getSubscriptionId());
        }
    }

    void close() throws JMSException {
        subscriber.close();
        session.close();
        connection.close();
    }
}
