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

import org.wso2.andes.jms.Session;

import java.util.Properties;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * A sample client that uses per message acknowledgement.
 */
public class SampleDelayedRedeliverySubscriber {

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "qpidConnectionfactory";
    String userName = "admin";
    String password = "admin";
    private static String CARBON_CLIENT_ID = "carbon";
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";
    private static String CARBON_DEFAULT_HOSTNAME = "localhost";
    private static String CARBON_DEFAULT_PORT = "5672";
    String queueName = "delayedDeliveryQueue";
    private QueueConnection queueConnection;
    private QueueSession queueSession;

    /**
     * Registers subscriber to a queue.
     *
     * @return The message consumer object of the subscriber.
     * @throws NamingException
     * @throws JMSException
     */
    public MessageConsumer registerSubscriber() throws NamingException, JMSException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("queue." + queueName, queueName);
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        // Here we need to use org.wso2.andes.jms.Session.PER_MESSAGE_ACKNOWLEDGE or the value 259.
        queueSession = queueConnection.createQueueSession(false, Session.PER_MESSAGE_ACKNOWLEDGE);
        // Create a message consumer
        Queue queue = (Queue) ctx.lookup(queueName);
        return queueSession.createConsumer(queue);
    }

    /**
     * Receives messages using the consumer.
     *
     * @param consumer The message consumer object of the subscriber.
     * @throws NamingException
     * @throws JMSException
     */
    public void receiveMessages(MessageConsumer consumer, boolean ackMessage) throws NamingException, JMSException {
        TextMessage message = (TextMessage) consumer.receive();
        System.out.println("Got message from queue receiver==>" + message.getText());
        if (ackMessage) {
            System.out.println("Acking message==>" + message.getText());
            message.acknowledge();
        }
    }

    /**
     * Closes down the subscriber.
     *
     * @param consumer The consumer object.
     * @throws JMSException
     */
    public void stopClient(MessageConsumer consumer) throws JMSException {
        // Housekeeping
        consumer.close();
        queueSession.close();
        queueConnection.stop();
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
