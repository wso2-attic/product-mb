package org.wso2.mb.integration.common.clients.operations.utils;

/**
 *
 */
public class JMSMessageHeader {
    private String jmsCorrelationID;
    private int jmsDeliveryMode;
    private long jmsExpiration;
    private String jmsMessageID;
    private int jmsPriority;
    private boolean jmsRedelivered;
    private long jmsTimestamp;
    private String jmsType;

    public String getJmsCorrelationID() {
        return jmsCorrelationID;
    }

    public void setJmsCorrelationID(String jmsCorrelationID) {
        this.jmsCorrelationID = jmsCorrelationID;
    }

    public int getJmsDeliveryMode() {
        return jmsDeliveryMode;
    }

    public void setJmsDeliveryMode(int jmsDeliveryMode) {
        this.jmsDeliveryMode = jmsDeliveryMode;
    }

    public long getJmsExpiration() {
        return jmsExpiration;
    }

    public void setJmsExpiration(long jmsExpiration) {
        this.jmsExpiration = jmsExpiration;
    }

    public String getJmsMessageID() {
        return jmsMessageID;
    }

    public void setJmsMessageID(String jmsMessageID) {
        this.jmsMessageID = jmsMessageID;
    }

    public int getJmsPriority() {
        return jmsPriority;
    }

    public void setJmsPriority(int jmsPriority) {
        this.jmsPriority = jmsPriority;
    }

    public boolean isJmsRedelivered() {
        return jmsRedelivered;
    }

    public void setJmsRedelivered(boolean jmsRedelivered) {
        this.jmsRedelivered = jmsRedelivered;
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
}
