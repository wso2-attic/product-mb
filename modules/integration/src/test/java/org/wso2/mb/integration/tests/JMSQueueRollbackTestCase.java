/**
 * Copyright (c) 2009, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.mb.integration.tests;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.Test;
import org.wso2.carbon.event.client.broker.BrokerClient;
import org.wso2.carbon.integration.framework.LoginLogoutUtil;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * Test Case for roll backing the JMS Transaction. In this JMS Sender
 * sends a message and there is a JMS Listener registered to listen to
 * a queue. Once a message is received in to the listener, it will roll back the
 * transaction and message needs to be redelivered to the listener in the next
 * try.
 */
public class JMSQueueRollbackTestCase {

    private LoginLogoutUtil util = new LoginLogoutUtil();
    private static final Log log = LogFactory.getLog(JMSQueueRollbackTestCase.class);
    private BrokerClient brokerClient;

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String QUEUE_NAME_PREFIX = "queue.";
    private static final String CF_NAME = "qpidConnectionfactory";
    String userName = "admin";
    String password = "admin";
    String queueName = "rollBackTestQueue";
    private static final int MAXIMUM_WAIT_ITERATION = 10;



     @Test(groups = {"wso2.mb"})
    public void receiveMessages() throws NamingException, JMSException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("queue." + queueName, queueName);
        System.out.println("getTCPConnectionURL(userName,password) = " + getTCPConnectionURL(userName, password));
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        QueueConnection queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        QueueSession queueSession =
                queueConnection.createQueueSession(true, Session.SESSION_TRANSACTED);

        //Receive message
        Queue queue = (Queue) ctx.lookup(queueName);
        MessageConsumer queueReceiver = queueSession.createConsumer(queue);

        SampleMessageListener listener = new SampleMessageListener(queueConnection, queueSession, queueReceiver, true);
        queueReceiver.setMessageListener(listener);

        sendMessages();

        Assert.assertEquals(true, isRollBackEventsReceivedAgain(listener));

        queueReceiver.close();
        queueSession.close();
        queueConnection.stop();
        queueConnection.close();
    }

    private boolean isRollBackEventsReceivedAgain(SampleMessageListener listener) {
        int count = 0;

        //5 secs sleep to receive the new meesage and perform rollback
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ignored) {
        }

        int initialMessageCount = listener.getMessageCount();


        while (count < MAXIMUM_WAIT_ITERATION) {
            if(listener.getMessageCount()>initialMessageCount){
                return true;
            }
            try {
                Thread.sleep(300000);
            } catch (InterruptedException ignored) {
            }
            count++;
        }
        return false;
    }



    public void sendMessages() throws NamingException, JMSException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put(QUEUE_NAME_PREFIX + queueName, queueName);
        System.out.println("getTCPConnectionURL(userName,password) = " + getTCPConnectionURL(userName, password));
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        QueueConnection queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        QueueSession queueSession =
                queueConnection.createQueueSession(true, QueueSession.SESSION_TRANSACTED);
        // Send message
        Queue queue = (Queue) ctx.lookup(queueName);
        // create the message to send
        TextMessage textMessage = queueSession.createTextMessage("test");
        javax.jms.QueueSender queueSender = queueSession.createSender(queue);
        queueSender.send(textMessage);
        queueSender.close();
        queueSession.close();
        queueConnection.close();
    }

    private String getTCPConnectionURL(String username, String password) {
        String carbonClientId = "carbon";
        String carbonVirtualHostName = "carbon";
        String carbonDefaultHostname = "localhost";
        String carbonDefaultPort = "5672";
        return new StringBuffer()
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(carbonClientId)
                .append("/").append(carbonVirtualHostName)
                .append("?brokerlist='tcp://").append(carbonDefaultHostname)
                .append(":").append(carbonDefaultPort).append("'")
                .toString();
    }

}
