/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
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

package org.wso2.mb.integration.common.clients;

public class AndesClientUtils {


    public static final String ANDES_INITIAL_CONTEXT_FACTORY = "org.wso2.andes.jndi" +
                                                               ".PropertiesFileInitialContextFactory";
    public static final int ANDES_DEFAULT_PORT = 5672;

    /**
     * Generate broker connection string
     *
     * @param userName
     *         Username
     * @param password
     *         Password
     * @param brokerHost
     *         Hostname of broker (E.g. localhost)
     * @param port
     *         Port (E.g. 5672)
     * @return Broker Connection String
     */
    public static String getBrokerConnectionString(String userName, String password,
                                                   String brokerHost, int port) {

        return "amqp://" + userName + ":" + password
               + "@clientID/carbon?brokerlist='tcp://"
               + brokerHost + ":" + port + "'";
    }
}
