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

    private int delay = 0;
    private int currentMsgCount = 0;

    public SampleMessageListener(int delay) {
        this.delay = delay;
    }
    public void onMessage(Message message) {
        TextMessage receivedMessage = (TextMessage) message;
        try {
            System.out.println("Got the message ==> " + (currentMsgCount+1) + " - "+ receivedMessage.getText());
            currentMsgCount++;

            if(delay != 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    //silently ignore
                }
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

}
