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


package org.sample.jms;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.naming.NamingException;


public class Main {

    public static void main(String[] args) throws NamingException, JMSException, InterruptedException {
        DurableTopicSubscriber durableTopicSubscriber = new DurableTopicSubscriber();
        durableTopicSubscriber.subscribe();
        TopicPublisher topicPublisher = new TopicPublisher();
        topicPublisher.publishMessage(5);
        Thread.sleep(5000);
        durableTopicSubscriber.stopSubscriber();
        TopicPublisher topicPublisher2 = new TopicPublisher();
        topicPublisher2.publishMessage(5);
        Thread.sleep(5000);
        DurableTopicSubscriber durableTopicSubscriber2 = new DurableTopicSubscriber();
        durableTopicSubscriber2.subscribe();
        TopicPublisher topicPublisher3 = new TopicPublisher();
        topicPublisher3.publishMessage(5);
        Thread.sleep(5000);
        durableTopicSubscriber2.stopSubscriber();

    }

}









