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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * XA Transaction Queue Publisher
 */
public class XAQueuePublisher {

    private final JMSClientHelper jmsHelper;

    public XAQueuePublisher(JMSClientHelper jmsHelper) {
        this.jmsHelper = jmsHelper;
    }

    /**
     * Publish to queue using XA transactions
     * @throws NamingException
     * @throws JMSException
     * @throws XAException
     */
    public void publish() throws NamingException, JMSException, XAException {

        InitialContext initialContext = jmsHelper.createInitialContext();
        XAConnectionFactory connectionFactory = (XAConnectionFactory) initialContext.lookup(jmsHelper.CF_NAME);

        XAConnection xaConnection = connectionFactory.createXAConnection();
        xaConnection.start();
        XASession xaSession = xaConnection.createXASession();

        // Get XAResource and JMS session from the XASession. XAResource is given to
        // the TM and JMS Session is given to the AP.
        XAResource xaResource = xaSession.getXAResource();
        Session session = xaSession.getSession();

        Destination xaTestQueue = (Destination) initialContext.lookup(jmsHelper.QUEUE_NAME);
        session.createQueue(jmsHelper.QUEUE_NAME);
        // Application Program
        MessageProducer producer = session.createProducer(xaTestQueue);
        // Transaction Manager
        Xid xid = jmsHelper.getNewXid();
        xaResource.start(xid, XAResource.TMNOFLAGS);
        // Application Program
        producer.send(session.createTextMessage("Test 1"));
        // Transaction Manager
        xaResource.end(xid, XAResource.TMSUCCESS);
        int result = xaResource.prepare(xid);

        if(result == XAResource.XA_OK ) {
            xaResource.commit(xid, false);
        }
        session.close();
        xaConnection.close();
    }
}
