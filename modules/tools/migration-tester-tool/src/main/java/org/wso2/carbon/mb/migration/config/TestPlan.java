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

@XmlRootElement(name = "testPlan")
public class TestPlan {

    private Admin admin;
    private Roles roles;
    private Users users;
    private Queues queues;
    private Topics topics;
    private String managementConsole;
    private ClientTruststore clientTruststore;

    public String getManagementConsole() {
        return managementConsole;
    }

    @XmlElement(name = "managementConsole")
    public void setManagementConsole(String managementConsole) {
        this.managementConsole = managementConsole;
    }

    public Admin getAdmin() {
        return admin;
    }

    public void setAdmin(Admin admin) {
        this.admin = admin;
    }

    public Roles getRoles() {
        return roles;
    }

    @XmlElement(name = "roles")
    public void setRoles(Roles roles) {
        this.roles = roles;
    }

    public Users getUsers() {
        return users;
    }

    @XmlElement(name = "users")
    public void setUsers(Users users) {
        this.users = users;
    }

    public Queues getQueues() {
        return queues;
    }

    @XmlElement(name = "queues")
    public void setQueues(Queues queues) {
        this.queues = queues;
    }

    public Topics getTopics() {
        return topics;
    }

    @XmlElement(name = "topics")
    public void setTopics(Topics topics) {
        this.topics = topics;
    }

    public ClientTruststore getClientTruststore() {
        return clientTruststore;
    }

    @XmlElement(name = "clientTruststore")
    public void setClientTruststore(ClientTruststore clientTruststore) {
        this.clientTruststore = clientTruststore;
    }
}
