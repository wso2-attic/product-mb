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
import javax.naming.NamingException;

/**
 * The following class contains a publisher transactional sample. This sample uses publisher transactions so that it
 * would help in recovering published messages in case if the server goes down. This helps to prevent message loss.
 */
public class MainClass {
    private static final Logger log = Logger.getLogger(MainClass.class);

    /**
     * The main method for the transactional publishing sample.
     *
     * @param args The arguments passed.
     * @throws NamingException
     * @throws JMSException
     */
    public static void main(String[] args) throws NamingException, JMSException {

        // Creating a message consumer
        QueueConsumer queueConsumer = new QueueConsumer("Transactional-Queue");

        // Creating a transactional message publisher
        TransactionalQueuePublisher transactionalQueuePublisher = new TransactionalQueuePublisher("Transactional-Queue");

        log.info("------Sample for Message Sending and Committing.------");

        // Publishes a messages
        transactionalQueuePublisher.sendMessage("My First Message.");

        // Attempts to receive a message. No messages were received here as the send message was not committed.
        queueConsumer.receiveMessage();

        // Publishes a messages
        transactionalQueuePublisher.sendMessage("My Second Message.");

        // Committing all published messages.
        transactionalQueuePublisher.commitMessages();

        // Receives a message.
        queueConsumer.receiveMessage();

        // Receives a message.
        queueConsumer.receiveMessage();

        log.info("------Sample for Message Sending, Rollback and Committing.------");

        // Publishes a messages
        transactionalQueuePublisher.sendMessage("My Third Message.");

        // Attempts to receive a message. No messages were received here as the sent message was not committed.
        queueConsumer.receiveMessage();

        // Rollbacks all published messages. This can be used in-case if the server has gone down and in need of
        // recovering published messages.
        transactionalQueuePublisher.rollbackMessages();

        // Publishes a messages
        transactionalQueuePublisher.sendMessage("My Forth Message.");

        // Committing all published messages.
        transactionalQueuePublisher.commitMessages();

        // Receives a message.
        queueConsumer.receiveMessage();

        // Attempts to receive a message. No messages were received here as all the messages were received.
        queueConsumer.receiveMessage();

        // Shutting down the sample.
        System.exit(0);
    }
}