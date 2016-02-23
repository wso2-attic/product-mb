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
package org.wso2.carbon.andes.transports.mqtt.protocol.messages;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author andrea
 */
public class SubscribeMessage extends MessageIDMessage {

    /**
     * Interpretation of the bytes message
     */
    public static class Couple {

        private byte qos;
        private String topicFilter;

        public Couple(byte qos, String topic) {
            this.qos = qos;
            topicFilter = topic;
        }

        public byte getQos() {
            return qos;
        }

        public String getTopicFilter() {
            return topicFilter;
        }
    }
    private List<Couple> subscriptions = new ArrayList<Couple>();

    public SubscribeMessage() {
        messageType = SUBSCRIBE;
        qos = AbstractMessage.QOSType.LEAST_ONE;
    }
    
    public List<Couple> subscriptions() {
        return subscriptions;
    }

    public void addSubscription(Couple subscription) {
        subscriptions.add(subscription);
    }
}
