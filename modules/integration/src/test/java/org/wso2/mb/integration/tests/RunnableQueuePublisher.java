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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class RunnableQueuePublisher implements Runnable {

    private InitialContext ctx;
    private String queueName;
    private int messageCount;
    private static final String CF_NAME = "qpidConnectionfactory";
    private static final Log log = LogFactory.getLog(RunnableQueuePublisher.class);

    public RunnableQueuePublisher(InitialContext ctx, String queueName, int messageCount) {
        this.ctx = ctx;
        this.queueName = queueName;
        this.messageCount = messageCount;
    }

    @Override
    public void run() {
        try {
            QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
            QueueConnection queueConnection = connFactory.createQueueConnection();
            queueConnection.start();
            QueueSession queueSession =
                    queueConnection.createQueueSession(true, QueueSession.SESSION_TRANSACTED);
            Queue queue = (Queue) ctx.lookup(queueName);
            javax.jms.QueueSender queueSender = queueSession.createSender(queue);

            for (int count= 0 ; count < messageCount ; count++) {
                TextMessage textMessage = queueSession.createTextMessage("test");
                queueSender.send(textMessage);
            }
            log.info("Sent "+ messageCount + "number of messages to queue " + queueName);

            queueSender.close();
            queueSession.close();
            queueConnection.close();

        } catch (NamingException e) {
            log.error("Error in publishing messages to queue " + queueName, e);
        } catch (JMSException e) {
            log.error("Error in publishing messages to queue " + queueName, e);
        }
    }
}
