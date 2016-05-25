package org.wso2.carbon.andes.resource.manager.types;

import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;

import java.util.Date;

/**
 *
 */
public class Destination {
    private long id = 0;
    private String destinationName = null;
    private Date createdDate = null;
    private DestinationType destinationType = null;
    private ProtocolType protocol = null;
    private long messageCount = 0;
    private boolean isDurable = false;
    private String owner = null;
    private int subscriptionCount;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    public Date getCreatedDate() {
        return (Date) createdDate.clone();
    }

    public void setCreatedDate(Date createdDate) {
        this.createdDate = (Date) createdDate.clone();
    }

    public DestinationType getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(DestinationType destinationType) {
        this.destinationType = destinationType;
    }

    public ProtocolType getProtocol() {
        return protocol;
    }

    public void setProtocol(ProtocolType protocol) {
        this.protocol = protocol;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }

    public boolean isDurable() {
        return isDurable;
    }

    public void setDurable(boolean durable) {
        isDurable = durable;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public int getSubscriptionCount() {
        return subscriptionCount;
    }

    public void setSubscriptionCount(int subscriptionCount) {
        this.subscriptionCount = subscriptionCount;
    }
}
