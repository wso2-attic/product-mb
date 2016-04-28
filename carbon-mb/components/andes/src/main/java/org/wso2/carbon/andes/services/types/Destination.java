/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.carbon.andes.services.types;

import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;

import java.util.Date;

/**
 * This class represent a destination information object.
 */
public class Destination {
    private long id = 0;
    private String destinationName = null;
    private Date createdDate = null;
    private DestinationType destinationType = null;
    private ProtocolType protocolType = null;
    private long messageCount = 0;
    private boolean isDurable = false;

    //TODO : Use user object from user core
    private String owner = null;
    private int subscriptionCount;

    long getId() {
        return this.id;
    }

    String getDestinationName() {
        return this.destinationName;
    }

    Date getCreatedDate() {
        return this.createdDate;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public String getOwner() {
        return owner;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public boolean isDurable() {
        return isDurable;
    }

    public void setDurable(boolean durable) {
        isDurable = durable;
    }

    public void setSubscriptionCount(int subscriptionCount) {
        this.subscriptionCount = subscriptionCount;
    }

    public int getSubscriptionCount() {
        return subscriptionCount;
    }

    public DestinationType getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(DestinationType destinationType) {
        this.destinationType = destinationType;
    }

    public void setProtocolType(ProtocolType protocolType) {
        this.protocolType = protocolType;
    }
}
