/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.sample.jms;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.jms.MessageConsumer;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * This class contains methods and properties relate to Queue Receiver (Subscriber)
 */
public class SampleQueueSelectorReceiver {
    //JNDI Initial Context Factory. Don't change this
    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    //Connection factory prefix
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    //Connection factory name
    private static final String CF_NAME = "qpidConnectionfactory";
    //Queue prefix
    private static final String QUEUE_NAME_PREFIX = "queue.";
    //username
    String userName = "admin";
    //password
    String password = "admin";
    //Client id it can be something
    private static String CARBON_CLIENT_ID = "carbon";
    //MB's Virtual host name should be match with this, default name is "carbon" can be configured
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";
    //IP Address of the host
    private static String CARBON_DEFAULT_HOSTNAME = "localhost";
    //Standard AMQP port number
    private static String CARBON_DEFAULT_PORT = "5672";
    //Queue name
    String queueName = "testQueue";
    private QueueConnection queueConnection;
    private QueueSession queueSession;

    /**
     * Creating a Message Consumer Object
     *
     * @param selector         JMS Selector

     * @return Configured Message Consumer
     * @throws NamingException
     * @throws JMSException
     */
    public MessageConsumer registerSubscriber(String selector)
            throws NamingException, JMSException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put(QUEUE_NAME_PREFIX+ queueName, queueName);
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        // Create a JMS connection
        queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        // Create JMS session object
        queueSession =
                queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        // Look up a JMS queue
        Queue queue = (Queue) ctx.lookup(queueName);
        // Create JMS consumer
        MessageConsumer consumer = queueSession.createConsumer(queue, selector, false);
        System.out.println("Starting Queue Listener....");
        System.out.println("JMS Selector : " + selector);
        return consumer;
    }

    /**
     * Receive Messages
     *
     * @param consumer Message consumer
     * @throws NamingException
     * @throws JMSException
     */
    public void receiveMessages(MessageConsumer consumer) throws NamingException, JMSException {

        TextMessage message = (TextMessage) consumer.receive();
        System.out.println("Got message from queue receiver with conforming to selectors ==>" + message.getText());

        // Housekeeping
        consumer.close();
        queueSession.close();
        queueConnection.stop();
        queueConnection.close();
    }

    /**
     * To construct Connection AMQP URL
     *
     * @param username username
     * @param password password
     * @return AMQP Connection URL
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
