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

package org.wso2.carbon.mb.migration.admin.services;

import org.apache.axis2.AxisFault;
import org.apache.axis2.client.Options;
import org.apache.axis2.client.ServiceClient;
import org.wso2.carbon.andes.event.stub.core.TopicRolePermission;
import org.wso2.carbon.andes.event.stub.service.AndesEventAdminServiceEventAdminException;
import org.wso2.carbon.andes.event.stub.service.AndesEventAdminServiceStub;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.andes.stub.AndesAdminServiceStub;
import org.wso2.carbon.andes.stub.admin.types.Queue;
import org.wso2.carbon.andes.stub.admin.types.QueueRolePermission;
import org.wso2.carbon.um.ws.api.stub.PermissionDTO;
import org.wso2.carbon.um.ws.api.stub.RemoteUserStoreManagerServiceStub;
import org.wso2.carbon.um.ws.api.stub.RemoteUserStoreManagerServiceUserStoreExceptionException;

import java.rmi.RemoteException;
import java.util.List;

class AdminServiceWrapper {

    private RemoteUserStoreManagerServiceStub remoteUserStoreManagerServiceStub;
    private AndesAdminServiceStub andesAdminServiceStub;
    private AndesEventAdminServiceStub andesEventAdminServiceStub;

    AdminServiceWrapper(String backEndUrl, String sessionCookie) throws AxisFault {
        String userManagementEndpoint = backEndUrl + "/services/" + "RemoteUserStoreManagerService";
        String andesAdminEndpoint = backEndUrl + "/services/" + "AndesAdminService";
        String andesEventEndpoint = backEndUrl + "/services/" + "AndesEventAdminService";
        remoteUserStoreManagerServiceStub = new RemoteUserStoreManagerServiceStub(userManagementEndpoint);
        andesAdminServiceStub = new AndesAdminServiceStub(andesAdminEndpoint);
        andesEventAdminServiceStub = new AndesEventAdminServiceStub(andesEventEndpoint);

        authenticateStub(sessionCookie, remoteUserStoreManagerServiceStub._getServiceClient());
        authenticateStub(sessionCookie, andesAdminServiceStub._getServiceClient());
        authenticateStub(sessionCookie, andesEventAdminServiceStub._getServiceClient());
    }

    private void authenticateStub(String sessionCookie, ServiceClient serviceClient) {
        Options option;
        option = serviceClient.getOptions();
        option.setManageSession(true);
        option.setProperty(org.apache.axis2.transport.http.HTTPConstants.COOKIE_STRING, sessionCookie);
    }


    String[] listUsers()
            throws RemoteException, RemoteUserStoreManagerServiceUserStoreExceptionException {
        return remoteUserStoreManagerServiceStub.listUsers("*", 100);
    }

    String[] listRoles() throws RemoteUserStoreManagerServiceUserStoreExceptionException,
                                RemoteException {
        return remoteUserStoreManagerServiceStub.getRoleNames();
    }

    void addUser(String username, String password, String[] roles)
            throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException {
        remoteUserStoreManagerServiceStub.addUser(username, password, roles, null, null, false);
    }

    void addRole(String roleName, List<String> permissionList)
            throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException {
        PermissionDTO[] permissionDTOS = new PermissionDTO[permissionList.size()];
        for (int i = 0; i < permissionList.size(); i++) {
            permissionDTOS[i] = new PermissionDTO();
            permissionDTOS[i].setResourceId(permissionList.get(i));
        }
        remoteUserStoreManagerServiceStub.addRole(roleName, null, permissionDTOS);
    }

    void createQueue(String queueName)
            throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        andesAdminServiceStub.addQueueAndAssignPermission(queueName, null);
    }

    void updateQueuePermissions(String queueName, QueueRolePermission... queueRolePermissions)
            throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        andesAdminServiceStub.updatePermission(queueName, queueRolePermissions);
    }

    void createTopic(String topicName)
            throws AndesEventAdminServiceEventAdminException, RemoteException {
        andesEventAdminServiceStub.addTopic(topicName);

    }

    void updateTopicPermission(String topicName, TopicRolePermission... permissions)
            throws AndesEventAdminServiceEventAdminException, RemoteException {
        andesEventAdminServiceStub.updatePermission(topicName, permissions);
    }

    void removeUser(String username)
            throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException {
        remoteUserStoreManagerServiceStub.deleteUser(username);
    }

    void removeRole(String roleName)
            throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException {
        remoteUserStoreManagerServiceStub.deleteRole(roleName);
    }

    void deleteQueue(String queueName) throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        andesAdminServiceStub.deleteQueue(queueName);
    }

    void deleteTopic(String topicName) throws AndesEventAdminServiceEventAdminException, RemoteException {
        andesEventAdminServiceStub.removeTopic(topicName);
    }

    void removeSubscriber(String subscriptionId, String topicName)
            throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        String subscriberQueueName = "carbon:" + subscriptionId;
        andesAdminServiceStub.deleteQueue(subscriberQueueName);
        andesAdminServiceStub.deleteTopicFromRegistry(topicName, subscriberQueueName);
    }

    Queue[] getQueueList() throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        return andesAdminServiceStub.getAllQueues();
    }

    long getPendingMessageCount(String subscriptionId)
            throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        return andesAdminServiceStub.getPendingMessageCount("carbon:" + subscriptionId);
    }
}
