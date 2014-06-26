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

package org.wso2.mb.integration.tests;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.Test;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * This test case will check the message redelivery feature provided by Andes. It will basically check if a message
 * is not acked client will be able to receive it for configured number of times and no more  for queues
 */
public class MessageRedeliveryTestCase {

    private static final Log log = LogFactory.getLog(JMSQueueSubscribePublishTestCase.class);

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String QUEUE_NAME_PREFIX = "queue.";
    private static final String CF_NAME = "qpidConnectionfactory";
    String userName = "admin";
    String password = "admin";
    String queueName = "messageRedeliveryTestingQueue";
    InitialContext ctx = null;

    private int maximumNumberOfDeliveryAttempts = 10;
    private int AckWaitTimeOut = 10000;


    @Test(groups = {"wso2.mb"})
    public void runJMSQueueSubscribePublishTestCase() {
        try {
            //initialize the context
            init();
            MessageConsumer msgConsumer = createSubscriber(queueName);

            RunnableQueuePublisher queuePublisher = new RunnableQueuePublisher(ctx, queueName, 1);


            Thread publisherThread = new Thread(queuePublisher);

            publisherThread.start();

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                //ignore
            }

            int tryCount = 0;

            int msgCount = 0;

            SampleMessageListener listener = (SampleMessageListener) msgConsumer.getMessageListener();
            msgCount = listener.getMessageCount();

            //we iterate until maximum delivery attempts limit is breached. Give up after 5 more tries
            while (tryCount < maximumNumberOfDeliveryAttempts + 5 && maximumNumberOfDeliveryAttempts > msgCount) {
                try {
                    Thread.sleep(AckWaitTimeOut * 2);
                } catch (InterruptedException ignored) {
                }
                msgCount = listener.getMessageCount();
                tryCount++;
            }

            //we have received enough messages. We should get no more.

            try {
                Thread.sleep(AckWaitTimeOut * 2);
            } catch (InterruptedException ignored) {
            }


            Assert.assertEquals(maximumNumberOfDeliveryAttempts, msgCount);

            //clear resources
            listener.stopMessageListener();


        } catch (NamingException e) {

            log.error("Error while running runJMSQueueSubscribePublishTestCase", e);

        } catch (JMSException e) {

            log.error("Error while running runJMSQueueSubscribePublishTestCase", e);

        }
    }

    private void init() throws NamingException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put(QUEUE_NAME_PREFIX + queueName, queueName);
        ctx = new InitialContext(properties);
    }

    private MessageConsumer createSubscriber(String queueName) throws NamingException, JMSException {

        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        QueueConnection queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        QueueSession queueSession =
                queueConnection.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);

        Queue q = (Queue) ctx.lookup(queueName);
        MessageConsumer queueReceiver = queueSession.createConsumer(q);

        //this listener will not acknowledge for the messages received
        SampleMessageListener listener = new SampleMessageListener(queueConnection, queueSession, queueReceiver, false);
        queueReceiver.setMessageListener(listener);

        return queueReceiver;
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
