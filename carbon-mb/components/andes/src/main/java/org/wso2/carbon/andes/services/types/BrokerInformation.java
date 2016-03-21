/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.carbon.andes.services.types;

/**
 * This class represent a broker information object.
 */
public class BrokerInformation {
    private String nodeID;
    private String ipAddress;
    private int amqpPort;
    private int amqpSecurePort;
    private int mqttPort;
    private int mqttSecurePort;
    private int thriftPort;
    private int hazelcastPort;

    public String getNodeID() {
        return nodeID;
    }

    public void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getAmqpPort() {
        return amqpPort;
    }

    public void setAmqpPort(int amqpPort) {
        this.amqpPort = amqpPort;
    }

    public int getAmqpSecurePort() {
        return amqpSecurePort;
    }

    public void setAmqpSecurePort(int amqpSecurePort) {
        this.amqpSecurePort = amqpSecurePort;
    }

    public int getMqttPort() {
        return mqttPort;
    }

    public void setMqttPort(int mqttPort) {
        this.mqttPort = mqttPort;
    }

    public int getMqttSecurePort() {
        return mqttSecurePort;
    }

    public void setMqttSecurePort(int mqttSecurePort) {
        this.mqttSecurePort = mqttSecurePort;
    }

    public int getThriftPort() {
        return thriftPort;
    }

    public void setThriftPort(int thriftPort) {
        this.thriftPort = thriftPort;
    }

    public int getHazelcastPort() {
        return hazelcastPort;
    }

    public void setHazelcastPort(int hazelcastPort) {
        this.hazelcastPort = hazelcastPort;
    }
}
