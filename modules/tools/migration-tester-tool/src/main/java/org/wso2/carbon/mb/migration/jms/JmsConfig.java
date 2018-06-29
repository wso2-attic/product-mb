/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.carbon.mb.migration.jms;

import javax.naming.InitialContext;

class JmsConfig {

    private String connectionFactoryName = "qpidConnectionFactory";
    private String destination = "TestQueue";
    private String subscriptionId = "";
    private String username = "admin";

    private String password;

    private InitialContext initialContext;

    JmsConfig(InitialContext context) {
        initialContext = context;
    }

    InitialContext getInitialContext() {
        return initialContext;
    }

    String getDestination() {
        return destination;
    }

    void setDestination(String destination) {
        this.destination = destination;
    }

    String getConnectionFactoryName() {
        return connectionFactoryName;
    }

    void setConnectionFactoryName(String connectionFactoryName) {
        this.connectionFactoryName = connectionFactoryName;
    }

    String getSubscriptionId() {
        return subscriptionId;
    }

    void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    String getUsername() {
        return username;
    }

    void setUsername(String username) {
        this.username = username;
    }

    String getPassword() {
        return password;
    }

    void setPassword(String password) {
        this.password = password;
    }
}
