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

import org.apache.log4j.Logger;

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
import java.util.Properties;

/**
 * This class contains methods which is used in creating and using a transactional JMS message publisher.
 */
public class TransactionalQueuePublisher {

    private static Logger log = Logger.getLogger(TransactionalQueuePublisher.class);

    /**
     * Andes initial context factory.
     */
    public static final String ANDES_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";

    /**
     * Connection factory name prefix.
     */
    public static final String CF_NAME_PREFIX = "connectionfactory.";

    /**
     * Andes connection factory name.
     */
    public static final String CF_NAME = "andesConnectionfactory";

    /**
     * The authorized username for the AMQP connection url.
     */
    private static final String userName = "admin";

    /**
     * The authorized password for the AMQP connection url.
     */
    private static final String password = "admin";

    /**
     * Client id for the AMQP connection url.
     */
    private static final String CARBON_CLIENT_ID = "carbon";

    /**
     * MB's Virtual host name should be match with this, default name is "carbon" can be configured.
     */
    private static final String CARBON_VIRTUAL_HOST_NAME = "carbon";

    /**
     * IP Address of the host for AMQP connection url.
     */
    private static final String CARBON_DEFAULT_HOSTNAME = "localhost";

    /**
     * Standard AMQP port number for the connection url.
     */
    private static final String CARBON_DEFAULT_PORT = "5672";

    /**
     * Queue prefix for initializing context.
     */
    private static final String QUEUE_NAME_PREFIX = "queue.";

    /**
     * The queue connection in which the messages would be published.
     */
    private QueueConnection queueConnection;

    /**
     * The queue session in which the messages would be published.
     */
    private QueueSession queueSession;

    /**
     * The queue in which the messages would be published.
     */
    private Queue queue;

    /**
     * Creates a transactional JMS publisher.
     *
     * @param queueName The name of the queue to which messages should be published.
     * @throws NamingException
     * @throws JMSException
     */
    public TransactionalQueuePublisher(String queueName) throws NamingException, JMSException {

        // Creating properties for the initial context
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, ANDES_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put(QUEUE_NAME_PREFIX + queueName, queueName);

        // Creating initial context
        InitialContext initialContext = new InitialContext(properties);

        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) initialContext.lookup(CF_NAME);

        // Create a JMS connection
        queueConnection = connFactory.createQueueConnection();
        queueConnection.start();

        // Create JMS session object. Here we mentioned that the messages will be published transactionally to the
        // broker.
        queueSession = queueConnection.createQueueSession(true, QueueSession.SESSION_TRANSACTED);

        // Look up a JMS queue
        queue = (Queue) initialContext.lookup(queueName);

        // Adding a shutdown hook listener
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    shutdownPublisher();
                } catch (JMSException jmsException) {
                    throw new RuntimeException(jmsException.getMessage(), jmsException);
                }
            }
        });
    }

    /**
     * Publishes a JMS message.
     *
     * @param messageContent The message content to publish.
     * @throws JMSException
     */
    public void sendMessage(String messageContent) throws JMSException {
        // Create the message to send
        TextMessage textMessage = queueSession.createTextMessage(messageContent);

        // Sending a message
        QueueSender queueSender = queueSession.createSender(queue);
        queueSender.send(textMessage);
        log.info("Message sent : " + textMessage.getText());
    }

    /**
     * Committing all messages that are being sent.
     *
     * @throws JMSException
     */
    public void commitMessages() throws JMSException {
        log.info("Committing messages.");
        queueSession.commit();
    }

    /**
     * Rollbacks all sent messages.
     *
     * @throws JMSException
     */
    public void rollbackMessages() throws JMSException {
        log.info("Rollbacks all uncommitted messages.");
        queueSession.rollback();
    }

    /**
     * Gets an AMQP connection string.
     *
     * @param username authorized username for the connection string.
     * @param password authorizes password for the connection string.
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

    /**
     * Shutting down the consumer.
     *
     * @throws JMSException
     */
    public void shutdownPublisher() throws JMSException {
        log.info("Shutting down publisher.");

        // Housekeeping
        if (null != queueSession) {
            queueSession.close();
        }
        if (null != queueConnection) {
            queueConnection.close();
        }
    }
}