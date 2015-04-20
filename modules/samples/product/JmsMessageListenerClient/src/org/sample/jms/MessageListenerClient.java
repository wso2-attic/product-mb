/*
 * Copyright 2004,2005 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sample.jms;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public class MessageListenerClient {

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "qpidConnectionfactory";
    String userName = "admin";
    String password = "admin";
    private static String CARBON_CLIENT_ID = "carbon";
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";
    private static String CARBON_DEFAULT_HOSTNAME = "localhost";
    private static String CARBON_DEFAULT_PORT = "5672";
    String topicName = "foo.bar";
    String queueName = "queue";

    public void registerSubscribers() throws NamingException,InterruptedException, JMSException {
        try {
            InitialContext ctx = initQueue();

            TopicConnectionFactory topicConnectionFactory =
                    (TopicConnectionFactory) ctx.lookup("qpidConnectionfactory");
            TopicConnection topicConnection = topicConnectionFactory.createTopicConnection();
            topicConnection.start();
            TopicSession topicSession =
                    topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

            Topic topic = (Topic) ctx.lookup(topicName);
            TopicSubscriber topicSubscriber =
                    topicSession.createSubscriber(topic);

            topicSubscriber.setMessageListener(new SampleMessageListener(topicConnection, topicSession, topicSubscriber));
            publishMessagesToTopic();

            Thread.sleep(5000);
            QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
            QueueConnection queueConnection = connFactory.createQueueConnection();
            queueConnection.start();
            QueueSession queueSession =
                    queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);

            Queue queue = (Queue) ctx.lookup(queueName);
            MessageConsumer queueReceiver = queueSession.createConsumer(queue);
            queueReceiver.setMessageListener(new SampleMessageListener(queueReceiver, queueSession, queueConnection));
            publishMessagesToQueue();

        } catch (NamingException e) {
            e.printStackTrace();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private void publishMessagesToTopic() throws NamingException, JMSException, InterruptedException {
        InitialContext ctx = initQueue();
        TopicConnectionFactory tConnectionFactory =
                (TopicConnectionFactory) ctx.lookup("qpidConnectionfactory");
        TopicConnection tConnection = tConnectionFactory.createTopicConnection();
        tConnection.start();
        TopicSession tSession =
                tConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = (Topic) ctx.lookup(topicName);
        javax.jms.TopicPublisher topicPublisher = tSession.createPublisher(topic);
        for (int i = 0; i < 10; i++) {
            TextMessage topicMessage = tSession.createTextMessage("Topic Message - " + (i + 1));
            topicPublisher.publish(topicMessage);
            Thread.sleep(1000);
        }
        tConnection.close();
        tSession.close();
        topicPublisher.close();
    }

    private void publishMessagesToQueue() throws NamingException, JMSException, InterruptedException {
        InitialContext ctx = initQueue();
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        QueueConnection connection = connFactory.createQueueConnection();
        connection.start();
        QueueSession session =
                connection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);

        Queue queue = (Queue) ctx.lookup(queueName);
        javax.jms.QueueSender queueSender = session.createSender(queue);
        for (int i = 0; i < 10; i++) {
            TextMessage queueMessage = session.createTextMessage(" Queue Message - " + (i + 1));
            queueSender.send(queueMessage);
            Thread.sleep(1000);
        }

        connection.close();
        session.close();
        queueSender.close();
    }

    private InitialContext initQueue() throws NamingException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("queue." + queueName, queueName);
        properties.put("topic." + topicName, topicName);
        InitialContext ctx = new InitialContext(properties);
        return ctx;
    }

    private String getTCPConnectionURL(String username, String password) {
        // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'
        return new StringBuffer()
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(CARBON_CLIENT_ID)
                .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(CARBON_DEFAULT_HOSTNAME).append(":").append(CARBON_DEFAULT_PORT).append("'")
                .toString();
    }


}
