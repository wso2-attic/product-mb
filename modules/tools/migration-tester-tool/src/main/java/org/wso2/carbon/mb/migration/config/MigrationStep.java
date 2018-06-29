/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.carbon.mb.migration.config;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "afterMigration")
public class MigrationStep {

    private Senders senders;

    private Receivers receivers;

    private Publishers publishers;

    private Subscribers subscribers;

    public Senders getSenders() {
        return senders;
    }

    @XmlElement(name = "senders")
    public void setSenders(Senders senders) {
        this.senders = senders;
    }

    public Receivers getReceivers() {
        return receivers;
    }

    @XmlElement(name = "receivers")
    public void setReceivers(Receivers receivers) {
        this.receivers = receivers;
    }

    public Publishers getPublishers() {
        return publishers;
    }

    @XmlElement(name = "publishers")
    public void setPublishers(Publishers publishers) {
        this.publishers = publishers;
    }

    public Subscribers getSubscribers() {
        return subscribers;
    }

    @XmlElement(name = "subscribers")
    public void setSubscribers(Subscribers subscribers) {
        this.subscribers = subscribers;
    }
}
