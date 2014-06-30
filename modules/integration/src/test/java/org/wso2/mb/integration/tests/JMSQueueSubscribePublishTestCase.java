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

public class JMSQueueSubscribePublishTestCase {

    private static final Log log = LogFactory.getLog(JMSQueueSubscribePublishTestCase.class);

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String QUEUE_NAME_PREFIX = "queue.";
    private static final String CF_NAME = "qpidConnectionfactory";
    String userName = "admin";
    String password = "admin";
    String Q1 = "queue1";
    String Q2 = "queue2";
    private static final int MAXIMUM_WAIT_ITERATION = 10;
    InitialContext ctx = null;


    @Test(groups = {"wso2.mb"})
    public void runJMSQueueSubscribePublishTestCase() {
        try {
            //initialize the context
            init();
            MessageConsumer msgConsumerQ11 = createSubscriber(Q1);
            MessageConsumer msgConsumerQ12 = createSubscriber(Q1);
            MessageConsumer msgConsumerQ21 = createSubscriber(Q2);

            RunnableQueuePublisher queuePublisherQ1 = new RunnableQueuePublisher(ctx, Q1, 10);
            RunnableQueuePublisher queuePublisherQ2 = new RunnableQueuePublisher(ctx, Q2, 10);

            Thread publisherThread1 = new Thread(queuePublisherQ1);
            Thread publisherThread2 = new Thread(queuePublisherQ2);

            publisherThread1.start();
            publisherThread2.start();

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                //ignore
            }

            int tryCount = 0;

            int msgCountQ11 = 0;
            int msgCountQ12 = 0;
            int msgCountQ21 = 0;

            while (tryCount < MAXIMUM_WAIT_ITERATION) {

                SampleMessageListener listenerQ11 = (SampleMessageListener)msgConsumerQ11.getMessageListener();
                msgCountQ11 = listenerQ11.getMessageCount();

                SampleMessageListener listenerQ12 = (SampleMessageListener)msgConsumerQ12.getMessageListener();
                msgCountQ12 = listenerQ12.getMessageCount();

                SampleMessageListener listenerQ21 = (SampleMessageListener)msgConsumerQ21.getMessageListener();
                msgCountQ21 = listenerQ21.getMessageCount();

                if (20 > (msgCountQ11 + msgCountQ12 + msgCountQ21)){
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException ignored) {
                    }
                }
                else{
                   return;
                }
                tryCount++;
            }

            Assert.assertEquals(20, (msgCountQ11 + msgCountQ12 + msgCountQ21));

            //clear resources
            SampleMessageListener listenerQ11 = (SampleMessageListener)msgConsumerQ11.getMessageListener();
            SampleMessageListener listenerQ12 = (SampleMessageListener)msgConsumerQ12.getMessageListener();
            SampleMessageListener listenerQ21 = (SampleMessageListener)msgConsumerQ21.getMessageListener();

            listenerQ11.stopMessageListener();
            listenerQ12.stopMessageListener();
            listenerQ21.stopMessageListener();


        }  catch (NamingException e) {

            log.error("Error while running runJMSQueueSubscribePublishTestCase", e);

        }  catch (JMSException e )  {

            log.error("Error while running runJMSQueueSubscribePublishTestCase", e);

        }
    }

    private void init() throws NamingException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put(QUEUE_NAME_PREFIX + Q1, Q1);
        properties.put(QUEUE_NAME_PREFIX + Q2, Q2);
        ctx = new InitialContext(properties);
    }
    private  MessageConsumer createSubscriber(String queueName) throws NamingException, JMSException {

        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        QueueConnection queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        QueueSession queueSession =
                queueConnection.createQueueSession(false,Session.AUTO_ACKNOWLEDGE);

        Queue q = (Queue) ctx.lookup(queueName);
        MessageConsumer queueReceiver = queueSession.createConsumer(q);

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
