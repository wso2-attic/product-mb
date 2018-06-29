/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.carbon.mb.migration.jms;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.naming.NamingException;

class JmsQueueSender {

    private final QueueConnection connection;
    private final QueueSession session;
    private final QueueSender sender;
    private final JmsConfig config;

    JmsQueueSender(JmsConfig config) throws JMSException, NamingException {
        InitialContext ctx = config.getInitialContext();
        // Lookup connection factory
        QueueConnectionFactory connFactory =
                (QueueConnectionFactory) ctx.lookup(config.getConnectionFactoryName());
        connection = connFactory.createQueueConnection(config.getUsername(), config.getPassword());
        connection.start();
        session = connection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);

        sender = session.createSender(null);
        this.config = config;
    }


    void send(String queueName, int messageCount) throws NamingException, JMSException {
        // Send message
        Queue queue = (Queue) config.getInitialContext().lookup(queueName);
        // create the message to send

        TextMessage textMessage = session.createTextMessage("Test Message Content");
        for (int i = 0; i < messageCount; i++) {
            textMessage.setJMSCorrelationID(String.valueOf(i));
            sender.send(queue, textMessage);
        }
    }

    void close() throws JMSException {
        session.close();
        connection.stop();
        connection.close();
    }
}
