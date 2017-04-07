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

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.xa.Xid;

/**
 * Util class with common helper methods when writing client code
 */
public class JMSClientHelper {

    private static String CARBON_CLIENT_ID = "carbon";
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";
    private static String CARBON_DEFAULT_HOSTNAME = "localhost";
    private static String CARBON_DEFAULT_PORT = "5672";

    final String QUEUE_NAME = "testQueue";
    final String ANDES_ICF  = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    final String CF_NAME_PREFIX = "connectionfactory.";
    final String CF_NAME = "andesConnectionfactory";

    public static AtomicLong GLOBAL_ID_GENERATOR =  new AtomicLong();

    /**
     * Return a different Xid each time this method is invoked
     *
     * @return Xid
     */
    public static Xid getNewXid() {
        return new TestXidImpl(100, toByteArray(GLOBAL_ID_GENERATOR.incrementAndGet()), new byte[] { 0x01 });
    }

    /**
     * Convert long to byte array
     *
     * @param value
     * @return
     */
    public static byte[] toByteArray(long value) {
        byte[] result = new byte[8];

        for(int i = 7; i >= 0; --i) {
            result[i] = (byte)((int)(value & 255L));
            value >>= 8;
        }
        return result;
    }


    /**
     * Create Initial Context for XA Sample
     *
     * @return InitialContext
     * @throws NamingException
     */
    public InitialContext createInitialContext() throws NamingException {

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, ANDES_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL("admin", "admin"));
        properties.put("queue."+ QUEUE_NAME, QUEUE_NAME);
        InitialContext initialContext = new InitialContext(properties);

        return initialContext;

    }

    /**
     * Create connection URL
     * @param username
     * @param password
     * @return Connection URL String
     */
    private String getTCPConnectionURL(String username, String password) {
        // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'
        return new StringBuffer()
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(CARBON_CLIENT_ID)
                .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(CARBON_DEFAULT_HOSTNAME).append(":").append(CARBON_DEFAULT_PORT).append("'")
                .toString();
    }
}