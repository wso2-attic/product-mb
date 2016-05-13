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

package org.wso2.carbon.andes.transports.config;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * Holds the configuration required for server startup
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class MqttTransportProperties {
    /**
     * holds the port of the server
     */
    @XmlAttribute
    private int port = 1883;

    /**
     * holds the host name of the server
     */
    @XmlAttribute
    private String host = "localhost";

    /**
     * number of acceptance threads
     */
    @XmlAttribute
    private int acceptanceThreads = 0;

    /**
     * number of worker threads
     */
    @XmlAttribute
    private int workerThreads = 0;

    /**
     * Defines the unique id for the transport
     */
    @XmlAttribute
    private String id = "mqtt";

    public MqttTransportProperties() {
    }

    public MqttTransportProperties(String host, int port, int acceptanceThreads, int workerThreads, String id) {
        this.host = host;
        this.port = port;
        this.acceptanceThreads = acceptanceThreads;
        this.workerThreads = workerThreads;
        this.id = id;
    }

    /**
     * Holds the name of the protocol
     */
    private String protocol;

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getHost() {
        return host;
    }

    public int getAcceptanceThreads() {
        return acceptanceThreads;
    }

    public void setAcceptanceThreads(int acceptanceThreads) {
        this.acceptanceThreads = acceptanceThreads;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public void setHost(String host) {

        this.host = host;
    }

    public int getPort() {
        return port;
    }
    public void setPort(int port) {
        this.port = port;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
