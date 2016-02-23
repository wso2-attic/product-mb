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

package org.wso2.carbon.andes.transports.mqtt.broker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.mqtt.broker.v311.MqttBroker;
import org.wso2.carbon.andes.transports.mqtt.protocol.Utils;
import org.wso2.carbon.andes.transports.server.Broker;

/**
 * Specifies each MQTT specification version supported by the broker and its corresponding implementation
 */
public enum BrokerVersion {

    /**
     * Specifies the implementation for mqtt 3.1 version
     */
    v3_0_0(new MqttBroker()),
    /**
     * Specifies the implementation for mqtt 3.1.1 version
     */
    v3_1_1(new MqttBroker()); //for the moment we reuse 3.1 broker classes for 3.1.1 as well

    private static final Log log = LogFactory.getLog(BrokerVersion.class);

    /**
     * We maintain the instance of the broker which corresponds to its version
     */
    private Broker broker;

    /**
     * Initializes the version of the broker
     *
     * @param broker the instance of the broker which corresponds to its version
     */
    BrokerVersion(Broker broker) {
        this.broker = broker;
    }

    /**
     * Gets a broker instance which corresponds to the specified enum
     *
     * @return the broker instance which corresponds to its version
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Broker getBroker() throws IllegalAccessException, InstantiationException {
        return broker;
    }

    /**
     * Get the corresponding broker version converting from its byte representation
     *
     * @param version the broker version
     * @return the version of the broker in its enum representation
     */
    public static BrokerVersion getBrokerVersion(byte version) {

        BrokerVersion brokerVersion;

        switch (version) {
            case Utils.VERSION_3_1:
                brokerVersion = v3_0_0;
                break;
            case Utils.VERSION_3_1_1:
                brokerVersion = v3_1_1;
                break;
            default:
                brokerVersion = v3_0_0;
                log.warn("Unknown protocol version " + version + " queried,will continue with the default version" +
                        " " +
                        "" + brokerVersion);
                break;
        }

        return brokerVersion;
    }


}
