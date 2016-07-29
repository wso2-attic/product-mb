/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import java.util.Properties;

/**
 * Sample Receiver to receive the messages which were not expired
 */
public class SampleQueueReceiver {
    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "qpidConnectionfactory";
    String userName = "admin";
    String password = "admin";
    private static String CARBON_CLIENT_ID = "carbon";
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";
    private static String CARBON_DEFAULT_HOSTNAME = "localhost";
    private static String CARBON_DEFAULT_PORT = "5672";
    String queueName = "expirationTestQueue";
    private QueueConnection queueConnection;
    private QueueSession queueSession;

    /**
     * Register Subscriber for a queue.
     *
     * @return MessageConsumer The message consumer object of the subscriber.
     * @throws NamingException
     * @throws JMSException
     */
    public MessageConsumer registerSubscriber() throws NamingException, JMSException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("queue."+ queueName,queueName);
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        queueSession = queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        //Receive message
        Queue queue =  (Queue) ctx.lookup(queueName);
        MessageConsumer consumer = queueSession.createConsumer(queue);
        return consumer;
    }

    /**
     * Receive messages using the consumer.
     *
     * @param consumer The message consumer object of the subscriber.
     * @throws NamingException
     * @throws JMSException
     */
    public void receiveMessages(MessageConsumer consumer) throws NamingException, JMSException {
        int receivedMessageCount = 0;
        //have 5 seconds as receive timeout value to stop the consumer
        while(null != consumer.receive(5000)){
            receivedMessageCount ++;
        }
        System.out.println("Received message count: " + receivedMessageCount);
    }

    /**
     * Close the connections at the end of operation
     * @param consumer The message consumer object of the subscriber.
     * @throws JMSException
     */
    public void closeConnections(MessageConsumer consumer) throws JMSException{
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
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(CARBON_CLIENT_ID)
                .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(CARBON_DEFAULT_HOSTNAME).append(":").append(CARBON_DEFAULT_PORT)
                .append("'")
                .toString();
    }
}
