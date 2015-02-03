package org.wso2.mb.integration.common.clients.operations.utils;

/**
 * Created by hemikakodikara on 1/26/15.
 */
public enum ExchangeType {
    QUEUE("queue"), TOPIC("topic");
    private String type;

    ExchangeType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
