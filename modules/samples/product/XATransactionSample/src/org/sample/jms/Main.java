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

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.transaction.xa.XAException;

/**
 * This sample will do following sub tasks while send/receive queue message via distribution transactions
 * 1. Initialize xa queue publisher.
 * 2. Publish queue message using xa transactions.
 * 3. Add queue subscription.
 * 4. Wait until message received to the subscriber.
 */
public class Main {

    public static void main(String[] args) throws NamingException, JMSException, InterruptedException, XAException {
        JMSClientHelper jmsHelper = new JMSClientHelper();
        // Publish to queue
        XAQueuePublisher xaQueuePub = new XAQueuePublisher(jmsHelper);
        xaQueuePub.publish();
        // Subscribe to queue
        XAQueueSubscriber xaQueueSub = new XAQueueSubscriber(jmsHelper);
        xaQueueSub.subscribe();
    }
}