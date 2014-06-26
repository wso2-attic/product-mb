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

import javax.jms.*;

public class QueueConsumer {
    private QueueConnection queueConnection;
    private QueueSession queueSession;
    private MessageConsumer queueReceiver;

    public QueueConsumer(QueueConnection queueConnection,QueueSession queueSession,MessageConsumer queueReceiver) {
        this.queueConnection = queueConnection;
        this.queueSession = queueSession;
        this.queueReceiver = queueReceiver;
    }
    public void consumeMessage() {
        try {
            TextMessage textMessage = (TextMessage) queueReceiver.receive();
            System.out.println("Got message ==> " + textMessage.getText());

            //House keeping
            queueReceiver.close();
            queueSession.close();
            queueConnection.stop();
            queueConnection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

}
