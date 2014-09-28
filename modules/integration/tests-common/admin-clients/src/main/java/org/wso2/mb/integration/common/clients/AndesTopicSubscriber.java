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
 * under the License. and limitations under the License.
 */

package org.wso2.mb.integration.common.clients;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * Client used to subscribe to topics in broker
 */
public class AndesTopicSubscriber {
    private final TopicConnectionFactory connectionFactory;
    private final Topic destination;
    private TopicConnection connection;
    private TopicSubscriber consumer;

    private AndesTopicSubscriber(Builder builder) throws NamingException {
        connectionFactory = (TopicConnectionFactory) builder.initialContext
                .lookup(Builder.connectionFactory);
        destination = (Topic) builder.initialContext.lookup(builder.destinationName);
    }

    /**
     * Connects to the configured broker.
     *
     * @throws javax.jms.JMSException
     */
    public void connect() throws JMSException {
        connection = connectionFactory.createTopicConnection();
        connection.start();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createSubscriber(destination);
    }

    /**
     * Disconnects from the broker.
     *
     * @throws javax.jms.JMSException
     */
    public void disconnect() throws JMSException {
        connection.close();
    }

    /**
     * Receives the next message produced for this message consumer.
     *
     * @return Message received
     * @throws javax.jms.JMSException
     */
    public javax.jms.Message receive() throws JMSException {
        return consumer.receive();
    }

    public static class Builder {
        public enum DestinationType {QUEUE, TOPIC};

        private static final String connectionFactory = "qpidConnectionfactory";

        // Required information to access broker
        private final String username;
        private final String password;
        private final String destinationName;

        // Initialized to default parameters
        String brokerHost = "localhost";
        int port = AndesClientUtils.ANDES_DEFAULT_PORT;
        private InitialContext initialContext;

        public Builder(String username, String password, String destinationName) {
            this.username = username;
            this.password = password;
            this.destinationName = destinationName;
        }

        public Builder brokerHost(String hostname) {
            this.brokerHost = hostname;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public AndesTopicSubscriber build() throws NamingException {
            createInitialContext();
            return new AndesTopicSubscriber(this);
        }

        private void createInitialContext() throws NamingException {
            Properties contextProperties = createContextProperties();
            this.initialContext = new InitialContext(contextProperties);
        }

        private Properties createContextProperties() {
            Properties contextProperties = new Properties();
            contextProperties.put(Context.INITIAL_CONTEXT_FACTORY,
                                  AndesClientUtils.ANDES_INITIAL_CONTEXT_FACTORY);
            String connectionString = AndesClientUtils.getBrokerConnectionString(this.username,
                                                                                 this.password,
                                                                                 this.brokerHost,
                                                                                 this.port);
            contextProperties.put("connectionfactory." + connectionFactory,
                                  connectionString);
            contextProperties.put("topic." + this.destinationName, this.destinationName);
            return contextProperties;
        }
    }
}
