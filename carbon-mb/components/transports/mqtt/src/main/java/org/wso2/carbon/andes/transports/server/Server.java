/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.carbon.andes.transports.server;

import org.wso2.carbon.andes.transports.config.MqttTransportProperties;

/**
 * Creates a server and allows it to be started and stopped
 */
public interface Server {
    /**
     * Will start a netty server with the provided fields
     *
     * @param ctx the set of properties used to start the server i.e host, port
     * @throws BrokerException occurs when netty transport is not initialized properly
     */
    void start(MqttTransportProperties ctx) throws BrokerException;

    /**
     * Stops a given server instance gracefully
     */
    void stop();
}
