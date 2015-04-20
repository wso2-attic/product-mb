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
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * This class contains methods and properties relate to Queue Sender (Publisher)
 */
public class SampleQueueSender {
    //JNDI Initial Context Factory. Don't change this
    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    //Connection factory prefix
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    //Connection factory name
    private static final String CF_NAME = "qpidConnectionfactory";
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
    //Queue prefix
    private static final String QUEUE_NAME_PREFIX = "queue.";
    //Queue name
    String queueName = "testQueue";
    private QueueConnection queueConnection;
    private QueueSession queueSession;

    /**
     * This method is used to Create Queue Sender and send messages
     *
     * @throws NamingException
     * @throws JMSException
     */
    public void sendMessages() throws NamingException, JMSException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put(QUEUE_NAME_PREFIX + queueName, queueName);
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        // Create a JMS connection
        queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        // Create JMS session object
        queueSession = queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        // Look up a JMS queue
        Queue queue = (Queue) ctx.lookup(queueName);
        // Create the message to send
        System.out.println("Starting Queue Sender....");
        TextMessage textMessage = queueSession.createTextMessage("Test Message Content with properties LK & 1");
        // Create JMS String Property in text message
        textMessage.setStringProperty("Currency", "LK");
        // Create JMS Integer Property in text message
        textMessage.setIntProperty("quantity", 1);
        // Create JMS consumer
        javax.jms.QueueSender queueSender = queueSession.createSender(queue);
        queueSender.send(textMessage);
        System.out.println("Send Message from QueueSender : Currency = LK , Quantity = 1 ");
        // Send message
        // Create the message to send
        textMessage = queueSession.createTextMessage("Test Message Content with properties USD");
        textMessage.setStringProperty("Currency", "USD");
        queueSender.send(textMessage);
        System.out.println("Send Message from QueueSender : Currency = USD ");
        // Send message
        // Create the message to send
        textMessage = queueSession.createTextMessage("Test Message Content with properties LK & 4");
        textMessage.setStringProperty("Currency", "LK");
        textMessage.setIntProperty("quantity", 4);
        queueSender.send(textMessage);
        System.out.println("Send Message from QueueSender : Currency = LK , Quantity = 4 ");
        // Send message
        // Create the message to send
        textMessage = queueSession.createTextMessage("Test Message Content with properties EUR");
        textMessage.setStringProperty("Currency", "EUR");
        queueSender.send(textMessage);
        System.out.println("Send Message from QueueSender : Currency = EUR ");
        // Send message
        // Create the message to send
        textMessage = queueSession.createTextMessage("Test Message Content with properties LK & 5");
        textMessage.setStringProperty("Currency", "LK");
        textMessage.setIntProperty("quantity", 5);
        queueSender.send(textMessage);
        System.out.println("Send Message from QueueSender : Currency = LK , Quantity = 5 ");
        // Send message
        // Create the message to send
        textMessage = queueSession.createTextMessage("Test Message Content with properties LK & 6");
        textMessage.setStringProperty("Currency", "LK");
        textMessage.setIntProperty("quantity", 6);
        System.out.println("Send Message from QueueSender : Currency = LK , Quantity = 6 ");
        queueSender.send(textMessage);
        // Send message
        // Create the message to send
        textMessage = queueSession.createTextMessage("Test Message Content without properties");
        queueSender.send(textMessage);
        queueSender.close();
        queueSession.close();
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
