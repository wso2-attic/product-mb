/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sample.jms;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.naming.NamingException;

/**
 * Sample executor class for message TTL
 */
public class Main {

    public static void main(String[] args) throws NamingException, JMSException {

        SampleQueueReceiver queueReceiver = new SampleQueueReceiver();
        MessageConsumer consumer = queueReceiver.registerSubscriber();

        //Send messages with very less time to live value
        System.out.println("Sending 5 messages with TTL value of 1sec");
        SampleQueueSender queueSenderWithTTL = new SampleQueueSender();
        queueSenderWithTTL.sendMessages(5,1000);
        queueReceiver.receiveMessages(consumer);

        //send messages without time to live value
        System.out.println("Sending 5 messages without TTL");
        SampleQueueSender queueSenderWithoutTTL = new SampleQueueSender();
        queueSenderWithoutTTL.sendMessages(5,0);
        queueReceiver.receiveMessages(consumer);

        //send messages with considerable time to live value
        System.out.println("Sending 5 messages TTL value of 10sec");
        SampleQueueSender queueSenderWithMediumTTL = new SampleQueueSender();
        queueSenderWithMediumTTL.sendMessages(5,10000);
        queueReceiver.receiveMessages(consumer);
        //close the connection
        queueReceiver.closeConnections(consumer);
    }

}
