package org.wso2.mb.integration.tests;

/**
 * Copyright (c) 2009, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;

public class SampleMessageListener implements MessageListener {

    private QueueConnection queueConnection;
    private QueueSession queueSession;
    private MessageConsumer queueReceiver;
    private int messageCount = 0;
    private boolean isToRollback;
    private static final Log log = LogFactory.getLog(JMSQueueRollbackTestCase.class);

    public SampleMessageListener(QueueConnection queueConnection, QueueSession queueSession,
                                 MessageConsumer queueReceiver,  boolean isToRollback) {
        this.queueConnection = queueConnection;
        this.queueSession = queueSession;
        this.queueReceiver = queueReceiver;
        this.isToRollback = isToRollback;
    }

    public void onMessage(Message message) {
        messageCount++;
        TextMessage receivedMessage = (TextMessage) message;
        try {

            log.info("Got the message ==> " + receivedMessage.getText());
            log.info("COUNT = " + messageCount);
            if(isToRollback) {
                queueSession.rollback();
            }
        } catch (NumberFormatException e) {
            log.error(e.getMessage(), e);
        } catch (JMSException e) {
            log.error(e.getMessage(), e);
        }
    }

    public int getMessageCount() {
        return messageCount;
    }

    public void stopMessageListener() {
        try {
            queueReceiver.close();
            queueSession.close();
            queueConnection.stop();
            queueConnection.close();
        } catch (JMSException e) {
            log.error("Error in closing the queue subscriber", e);
        }

    }
}

