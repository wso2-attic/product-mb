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

package org.wso2.carbon.transport.tests.mqtt.broker.v311.dataprovider.commands;

import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * Message data which will delegated from data provider to test case
 */
public class Message {
    /**
     * Specifies  the connect message the test should be executed against
     */
    private AbstractMessage message;

    /**
     * Properties which will be specific to the message
     */
    private Map<String, String> properties = new HashMap<>();

    /**
     * Specifies the expected result of the message
     */
    private Object expectedResult;

    public Message() {
        super();
    }

    public Message(AbstractMessage commandMessage, Object expectedResult) {
        this.message = commandMessage;
        this.expectedResult = expectedResult;
    }

    /**
     * Adds a property which would represent in the message
     *
     * @param propertyName  the name of the property
     * @param propertyValue the value of the property
     */
    public void addMessageProperty(String propertyName, String propertyValue) {
        this.properties.put(propertyName, propertyValue);
    }

    /**
     * Gets a property value by its name
     *
     * @param propertyName the name of the property to be retrieved
     * @return the value of the property
     */
    public String getMessageProperty(String propertyName) {
        return this.properties.get(propertyName);
    }

    /**
     * Clears all properties in the message
     */
    public void clearProperties() {
        this.properties.clear();
    }

    public AbstractMessage getMessage() {
        return message;
    }

    public Object getExpectedResult() {
        return expectedResult;
    }
}
