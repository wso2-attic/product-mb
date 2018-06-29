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

import javax.jms.JMSException;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.naming.InitialContext;
import javax.naming.NamingException;

class JmsTopicPublisher {

    private final TopicConnection connection;
    private final TopicSession session;
    private final TopicPublisher publisher;

    private final JmsConfig config;

    JmsTopicPublisher(JmsConfig config) throws JMSException, NamingException {
        InitialContext ctx = config.getInitialContext();
        // Lookup connection factory
        TopicConnectionFactory connFactory =
                (TopicConnectionFactory) ctx.lookup(config.getConnectionFactoryName());
        connection = connFactory.createTopicConnection(config.getUsername(), config.getPassword());
        connection.start();
        session = connection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);

        publisher = session.createPublisher(null);
        this.config = config;
    }


    void send(String topicName, int messageCount) throws NamingException, JMSException {
        // Send message
        Topic topic = (Topic) config.getInitialContext().lookup(topicName);
        // create the message to send
        TextMessage textMessage = session.createTextMessage("Test Message Content");
        for (int i = 0; i < messageCount; i++) {
            textMessage.setJMSCorrelationID(String.valueOf(i));
            publisher.publish(topic, textMessage);
        }
    }

    void close() throws JMSException {
        connection.stop();
        connection.close();
    }

}
