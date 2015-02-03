package org.wso2.mb.integration.common.clients.operations.utils;

/**
 * Created by hemikakodikara on 1/26/15.
 */
public enum JMSMessageType {
    TEXT("text"), BYTE("byte"), MAP("map"), OBJECT("object"), STREAM("stream");
    private String type;

    JMSMessageType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}