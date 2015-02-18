/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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
package org.wso2.mb.integration.common.clients.operations.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * This class represents the JMS message which would to be used in selectors.
 */
public class JMSMessageHeader {
    /**
     * JMS message correlation ID.
     */
    private String jmsCorrelationID;

    /**
     * JMS message ID
     */
    private String jmsMessageID;

    /**
     * JMS message timestamp. Timestamp at which the message is being published.
     */
    private long jmsTimestamp;

    /**
     * JMS type. The header allows to store a custom value as a string.
     */
    private String jmsType;

    /**
     * Map to store custom string properties.
     */
    Map<String, String> stringProperties = new HashMap<String, String>();

    /**
     * Map to store custom integer properties.
     */
    Map<String, Integer> integerProperties = new HashMap<String, Integer>();

    /**
     * Gets JMS message correlation ID.
     *
     * @return The correlation ID.
     */
    public String getJmsCorrelationID() {
        return jmsCorrelationID;
    }

    /**
     * Sets JMS message correlation ID.
     *
     * @param jmsCorrelationID The correlation ID.
     */
    public void setJmsCorrelationID(String jmsCorrelationID) {
        this.jmsCorrelationID = jmsCorrelationID;
    }

    /**
     * Gets the JMS message ID.
     *
     * @return The JMS message ID.
     */
    public String getJmsMessageID() {
        return jmsMessageID;
    }

    /**
     * Sets JMS message ID.
     *
     * @param jmsMessageID The jms message ID.
     */
    public void setJmsMessageID(String jmsMessageID) {
        this.jmsMessageID = jmsMessageID;
    }

    /**
     * Gets the JMS message timestamp.
     *
     * @return The JMS message timestamp at which it was published.
     */
    public long getJmsTimestamp() {
        return jmsTimestamp;
    }

    /**
     * Sets the JMS message timestamp.
     *
     * @param jmsTimestamp The JMS message timestamp at which it is published.
     */
    public void setJmsTimestamp(long jmsTimestamp) {
        this.jmsTimestamp = jmsTimestamp;
    }

    /**
     * Gets JMS type.
     *
     * @return The JMS type
     */
    public String getJmsType() {
        return jmsType;
    }

    /**
     * Sets JMS type.
     *
     * @param jmsType The JMS type.
     */
    public void setJmsType(String jmsType) {
        this.jmsType = jmsType;
    }

    /**
     * Gets custom string properties for message headers.
     *
     * @return The custom string properties for message headers.
     */
    public Map<String, String> getStringProperties() {
        return stringProperties;
    }

    /**
     * Sets custom string properties for message headers.
     *
     * @param stringProperties The custom string properties for message headers.
     */
    public void setStringProperties(Map<String, String> stringProperties) {
        this.stringProperties = stringProperties;
    }

    /**
     * Gets custom integer properties for message headers.
     *
     * @return The custom string properties for message headers.
     */
    public Map<String, Integer> getIntegerProperties() {
        return integerProperties;
    }

    /**
     * Sets custom string properties for message headers.
     *
     * @param integerProperties The custom string properties for message headers.
     */
    public void setIntegerProperties(Map<String, Integer> integerProperties) {
        this.integerProperties = integerProperties;
    }
}
