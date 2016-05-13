/*
 * Copyright (c) 2012-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages;

/**
 * The attributes Qos, Dup and Retain aren't used for Connect message
 *
 * @author andrea
 */
public class ConnectMessage extends AbstractMessage {
    String protocolName;
    byte procotolVersion;

    //Connection flags
    boolean cleanSession;
    boolean willFlag;
    byte willQos;
    boolean willRetain;
    boolean passwordFlag;
    boolean userFlag;
    int keepAlive;

    //Variable part
    String username;
    String password;
    String clientID;
    String willtopic;
    String willMessage;

    public ConnectMessage() {
        messageType = CONNECT;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public int getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(int keepAlive) {
        this.keepAlive = keepAlive;
    }

    public boolean isPasswordFlag() {
        return passwordFlag;
    }

    public void setPasswordFlag(boolean passwordFlag) {
        this.passwordFlag = passwordFlag;
    }

    public byte getProcotolVersion() {
        return procotolVersion;
    }

    public void setProcotolVersion(byte procotolVersion) {
        this.procotolVersion = procotolVersion;
    }

    public String getProtocolName() {
        return protocolName;
    }

    public void setProtocolName(String protocolName) {
        this.protocolName = protocolName;
    }

    public boolean isUserFlag() {
        return userFlag;
    }

    public void setUserFlag(boolean userFlag) {
        this.userFlag = userFlag;
    }

    public boolean isWillFlag() {
        return willFlag;
    }

    public void setWillFlag(boolean willFlag) {
        this.willFlag = willFlag;
    }

    public byte getWillQos() {
        return willQos;
    }

    public void setWillQos(byte willQos) {
        this.willQos = willQos;
    }

    public boolean isWillRetain() {
        return willRetain;
    }

    public void setWillRetain(boolean willRetain) {
        this.willRetain = willRetain;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public String getWillTopic() {
        return willtopic;
    }

    public void setWillTopic(String topic) {
        this.willtopic = topic;
    }

    public String getWillMessage() {
        return willMessage;
    }

    public void setWillMessage(String willMessage) {
        this.willMessage = willMessage;
    }

    @Override
    public String toString() {
        String base = String.format("Connect [clientID: %s, prot: %s, ver: %02X, clean: %b]", clientID,
                protocolName, procotolVersion, cleanSession);
        if (willFlag) {
            base += String.format(" Will [QoS: %d, retain: %b]", willQos, willRetain);
        }
        return base;
    }
}
