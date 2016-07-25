/*
 * Copyright (c) 2016 WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.naming.NamingException;

/**
 * Sample executor class for delayed redelivery.
 */
public class Main {
    public static void main(String[] args) throws NamingException, JMSException {
        // AndesAckWaitTimeOut is the timeout value till reject is sent.
        System.setProperty("AndesAckWaitTimeOut", "2000");

        // AndesRedeliveryDelay is the delay for redelivered messages.
        System.setProperty("AndesRedeliveryDelay", "10000");

        // Create subscriber
        SampleDelayedRedeliverySubscriber queueReceiver = new SampleDelayedRedeliverySubscriber();
        MessageConsumer consumer = queueReceiver.registerSubscriber();
        // Create publisher
        SampleQueueSenderForDelayedRedeliverySample queueSender = new SampleQueueSenderForDelayedRedeliverySample();

        // Publishing messages
        queueSender.sendMessages("#1");
        queueSender.sendMessages("#2");
        queueSender.sendMessages("#3");
        queueSender.sendMessages("#4");
        queueSender.sendMessages("#5");

        // Stopping publisher
        queueSender.stopClient();
        System.out.println();
        printCurrentTime();
        System.out.println();

        // Acking message #1
        queueReceiver.receiveMessages(consumer, true);
        // Not acking message #2
        queueReceiver.receiveMessages(consumer, false);
        // Not acking message #3
        queueReceiver.receiveMessages(consumer, false);
        // Acking message #4
        queueReceiver.receiveMessages(consumer, true);
        // Acking message #5
        queueReceiver.receiveMessages(consumer, true);

        System.out.println();
        printCurrentTime();
        System.out.println();

        // Message #2 and #3 gets redelivered to the subscriber by the server as they were not acked.
        // Acking message #2
        queueReceiver.receiveMessages(consumer, true);
        // Acking message #3
        queueReceiver.receiveMessages(consumer, true);

        System.out.println();
        printCurrentTime();
        System.out.println();

        // Stopping subscriber
        queueReceiver.stopClient(consumer);
    }

    /**
     * Prints the current time.
     */
    private static void printCurrentTime() {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormat.setTimeZone(calendar.getTimeZone());

        System.out.println("Current time : " + dateFormat.format(calendar.getTime()));
    }
}