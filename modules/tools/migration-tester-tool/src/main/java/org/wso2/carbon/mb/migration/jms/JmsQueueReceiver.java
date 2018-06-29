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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.naming.InitialContext;
import javax.naming.NamingException;

class JmsQueueReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsQueueReceiver.class);

    private final QueueConnection connection;
    private final QueueSession session;
    private final QueueReceiver receiver;
    private int lastReadOffset;

    JmsQueueReceiver(JmsConfig config) throws NamingException, JMSException {
        InitialContext ctx = config.getInitialContext();
        // Lookup connection factory
        QueueConnectionFactory connFactory =
                (QueueConnectionFactory) ctx.lookup(config.getConnectionFactoryName());
        connection = connFactory.createQueueConnection(config.getUsername(), config.getPassword());
        connection.start();
        session = connection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        Queue queue = (Queue) ctx.lookup(config.getDestination());
        receiver = session.createReceiver(queue);
        this.lastReadOffset = 0;
    }

    void receive(int timeout, int numberOfMessages) throws JMSException {
        for (int i = 0; i < numberOfMessages; i++) {
            Message message = receiver.receive(timeout);
            int offset = Integer.parseInt(message.getJMSCorrelationID());
            if (offset > lastReadOffset) {
                lastReadOffset = offset;
            } else {
                LOGGER.error("Message received out of order. Current offset: {}. Last read offset: {}",
                             offset, lastReadOffset);
            }
        }
    }

    void close() throws JMSException {
        connection.stop();
        receiver.close();
        session.close();
        connection.close();
    }
}
