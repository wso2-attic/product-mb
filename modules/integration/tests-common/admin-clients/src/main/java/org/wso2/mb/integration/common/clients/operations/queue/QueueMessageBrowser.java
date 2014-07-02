/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.mb.integration.common.clients.operations.queue;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Enumeration;
import java.util.Properties;

public class QueueMessageBrowser {

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "andesConnectionfactory";
    private static final String CARBON_CLIENT_ID = "carbon";
    private static final String CARBON_VIRTUAL_HOST_NAME = "carbon";

    private String hostName = "localhost";
    private String port = "5672";
    private String connectionString = "";

    private QueueConnection queueConnection = null;
    private QueueSession queueSession = null;
    private QueueBrowser queueBrowser = null;

    private String queueName;
    private  int printNumberOfMessagesPer = 1;
    private  boolean isToPrintEachMessage = false;

    public QueueMessageBrowser (String host, String port, String userName, String password,String destination, int printNumberOfMessagesPer, boolean isToPrintMessage) {
        this.hostName = host;
        this.port = port;
        this.queueName = destination;
        this.printNumberOfMessagesPer = printNumberOfMessagesPer;
        this.isToPrintEachMessage = isToPrintMessage;

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("queue." + queueName, queueName);

        System.out.println("getTCPConnectionURL(userName,password) = " + getTCPConnectionURL(userName, password));

        try {
            InitialContext ctx = new InitialContext(properties);
            // Lookup connection factory
            QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
            queueConnection = connFactory.createQueueConnection();
            queueConnection.start();
            queueSession = queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
            Queue queue = (Queue) ctx.lookup(queueName);
            queueBrowser = queueSession.createBrowser(queue);

        } catch (NamingException e) {
            System.out.println("Error while looking up for queue" + e);
        } catch (JMSException ex) {
            System.out.println("Error while initializing queue connection" + ex);
        }

    }

    public int getMessageCount() {
        int numMsgs = 0;
        try {
            Enumeration e = queueBrowser.getEnumeration();
            // count number of messages
            while (e.hasMoreElements()) {
                Message message = (Message) e.nextElement();
                numMsgs++;
            }
            System.out.println("closing Queue Browser");
            queueBrowser.close();
            queueSession.close();
            queueConnection.close();
            System.out.println("done closing Queue Browser");
        }  catch (JMSException e) {
            System.out.println("Error while message browsing"+ e);
        }
        return numMsgs;
    }

    private String getTCPConnectionURL(String username, String password) {
        if(connectionString != null && !connectionString.equals("")) {
            return connectionString;
        } else {
            return new StringBuffer()
                    .append("amqp://").append(username).append(":").append(password)
                    .append("@").append(CARBON_CLIENT_ID)
                    .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                    .append("?brokerlist='tcp://").append(hostName).append(":").append(port).append("'")
                    .toString();
        }
    }
}
