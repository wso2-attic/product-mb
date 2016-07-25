/*
 * Copyright (c) 2016 WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sample.jms;

import java.util.Properties;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * A sample queue message publisher for per message ack sample.
 */
public class SampleQueueSenderForPerMessageAckSample {

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String QUEUE_NAME_PREFIX = "queue.";
    private static final String CF_NAME = "qpidConnectionfactory";
    String userName = "admin";
    String password = "admin";
    private static String CARBON_CLIENT_ID = "carbon";
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";
    private static String CARBON_DEFAULT_HOSTNAME = "localhost";
    private static String CARBON_DEFAULT_PORT = "5672";
    String queueName = "perMessagePublishingQueue";
    private QueueConnection queueConnection;
    private QueueSession queueSession;
    private QueueSender queueSender;

    /**
     * Creates a queue publisher.
     *
     * @throws NamingException
     * @throws JMSException
     */
    public SampleQueueSenderForPerMessageAckSample() throws NamingException, JMSException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put(QUEUE_NAME_PREFIX + queueName, queueName);
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        queueSession = queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        // Send message
        Queue queue = (Queue) ctx.lookup(queueName);
        queueSender = queueSession.createSender(queue);
    }

    /**
     * Publishes a message with given content.
     *
     * @param messageContent The message content.
     * @throws JMSException
     */
    public void sendMessages(String messageContent) throws JMSException {
        System.out.println("Publishing message==>" + messageContent);
        TextMessage textMessage = queueSession.createTextMessage(messageContent);
        queueSender.send(textMessage);
    }

    /**
     * Shutdown the publisher client.
     *
     * @throws JMSException
     */
    public void stopClient() throws JMSException {
        queueSender.close();
        queueSession.close();
        queueConnection.close();
    }

    /**
     * Creates amqp url.
     *
     * @param username The username for the amqp url.
     * @param password The password for the amqp url.
     * @return AMQP url.
     */
    private String getTCPConnectionURL(String username, String password) {
        // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'
        return new StringBuffer()
                .append("amqp://")
                .append(username)
                .append(":")
                .append(password)
                .append("@")
                .append(CARBON_CLIENT_ID)
                .append("/")
                .append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(CARBON_DEFAULT_HOSTNAME)
                .append(":")
                .append(CARBON_DEFAULT_PORT)
                .append("'").toString();
    }
}
