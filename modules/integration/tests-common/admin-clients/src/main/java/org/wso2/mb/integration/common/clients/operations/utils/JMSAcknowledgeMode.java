package org.wso2.mb.integration.common.clients.operations.utils;

/**
 *
 */
public enum JMSAcknowledgeMode {
    SESSION_TRANSACTED(0), AUTO_ACKNOWLEDGE(1), CLIENT_ACKNOWLEDGE(2), DUPS_OK_ACKNOWLEDGE(3);
    private int type;

    JMSAcknowledgeMode(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
