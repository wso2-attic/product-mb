/*
 * Copyright 2004,2005 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sample.jms;

import javax.jms.*;

public class SampleMessageListener implements MessageListener {

    private TopicConnection topicConnection;
    private TopicSession topicSession;
    private TopicSubscriber topicSubscriber;

    public SampleMessageListener(TopicConnection topicConnection,
                                 TopicSession topicSession,
                                 TopicSubscriber topicSubscriber) {
        this.topicConnection = topicConnection;
        this.topicSession = topicSession;
        this.topicSubscriber = topicSubscriber;
    }

    public void onMessage(Message message) {
        TextMessage receivedMessage = (TextMessage) message;
        try {
            System.out.println("Got the message ==> " + receivedMessage.getText());
            this.topicSubscriber.close();
            this.topicSession.close();
            this.topicConnection.stop();
            this.topicConnection.close();
            
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }
}
