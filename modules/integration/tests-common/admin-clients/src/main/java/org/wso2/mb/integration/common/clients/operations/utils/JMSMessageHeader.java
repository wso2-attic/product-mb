package org.wso2.mb.integration.common.clients.operations.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class JMSMessageHeader {
    private String jmsCorrelationID;
    private String jmsMessageID;
    private long jmsTimestamp;
    private String jmsType;
    Map<String, String> stringProperties = new HashMap<String, String>();
    Map<String, Integer> integerProperties = new HashMap<String, Integer>();

    public String getJmsCorrelationID() {
        return jmsCorrelationID;
    }

    public void setJmsCorrelationID(String jmsCorrelationID) {
        this.jmsCorrelationID = jmsCorrelationID;
    }

    public String getJmsMessageID() {
        return jmsMessageID;
    }

    public void setJmsMessageID(String jmsMessageID) {
        this.jmsMessageID = jmsMessageID;
    }

    public long getJmsTimestamp() {
        return jmsTimestamp;
    }

    public void setJmsTimestamp(long jmsTimestamp) {
        this.jmsTimestamp = jmsTimestamp;
    }

    public String getJmsType() {
        return jmsType;
    }

    public void setJmsType(String jmsType) {
        this.jmsType = jmsType;
    }

    public Map<String, String> getStringProperties() {
        return stringProperties;
    }

    public void setStringProperties(Map<String, String> stringProperties) {
        this.stringProperties =  stringProperties;
    }

    public Map<String, Integer> getIntegerProperties() {
        return integerProperties;
    }

    public void setIntegerProperties(Map<String, Integer> integerProperties) {
        this.integerProperties = integerProperties;
    }
}
