/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * XA Transaction Queue Subscriber.
 */
public class XAQueueSubscriber {

    private final JMSClientHelper jmsHelper;

    XAQueueSubscriber(JMSClientHelper jmsHelper) {
        this.jmsHelper = jmsHelper;
    }

    /**
     * Initialize subscribe to queue
     * @throws JMSException
     * @throws NamingException
     */
    public void subscribe() throws JMSException, NamingException {

        InitialContext initialContext = jmsHelper.createInitialContext();

        // subscribe and see if the message is received
        ConnectionFactory queueConnectionFactory = (ConnectionFactory) initialContext
                .lookup(jmsHelper.CF_NAME);
        Connection queueConnection = queueConnectionFactory.createConnection();
        queueConnection.start();
        Session queueSession = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination xaTestQueue = (Destination) initialContext.lookup(jmsHelper.QUEUE_NAME);
        MessageConsumer messageConsumer = queueSession.createConsumer(xaTestQueue);

        // wait 5 seconds until message received
        Message receivedMessage = messageConsumer.receive(5000);

        if(receivedMessage != null) {
            System.out.println("JMS message received.");
            System.out.println("Destination of received message :" + receivedMessage.getJMSDestination());
        }
        queueConnection.close();
    }
}