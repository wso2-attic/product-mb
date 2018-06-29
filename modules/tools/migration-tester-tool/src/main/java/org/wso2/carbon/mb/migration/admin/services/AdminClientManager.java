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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.andes.event.stub.core.TopicRolePermission;
import org.wso2.carbon.andes.event.stub.service.AndesEventAdminServiceEventAdminException;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.andes.stub.admin.types.QueueRolePermission;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.authenticator.stub.LogoutAuthenticationExceptionException;
import org.wso2.carbon.mb.migration.config.Publisher;
import org.wso2.carbon.mb.migration.config.Queue;
import org.wso2.carbon.mb.migration.config.Receiver;
import org.wso2.carbon.mb.migration.config.Role;
import org.wso2.carbon.mb.migration.config.Sender;
import org.wso2.carbon.mb.migration.config.Subscriber;
import org.wso2.carbon.mb.migration.config.TestPlan;
import org.wso2.carbon.mb.migration.config.Topic;
import org.wso2.carbon.mb.migration.config.User;
import org.wso2.carbon.mb.migration.config.UserRoles;
import org.wso2.carbon.um.ws.api.stub.RemoteUserStoreManagerServiceUserStoreExceptionException;

import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AdminClientManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminClientManager.class);

    private LoginClient loginClient;

    private Map<String, User> userMap = new HashMap<>();

    private final TestPlan testPlan;

    private final boolean userMapPopulated;

    private final AdminServiceWrapper adminServiceWrapper;

    public AdminClientManager(TestPlan testPlan) throws RemoteException, LoginAuthenticationExceptionException,
                                                        MalformedURLException {
        this.testPlan = testPlan;
        System.setProperty("javax.net.ssl.trustStore", testPlan.getClientTruststore().getFilePath());
        System.setProperty("javax.net.ssl.trustStorePassword", testPlan.getClientTruststore().getPassword());
        adminServiceWrapper = login(testPlan.getAdmin().getUsername(),
                                    testPlan.getAdmin().getPassword(),
                                    testPlan.getManagementConsole());
        addAdminToUserMap(testPlan);
        userMapPopulated = false;

    }

    private void addAdminToUserMap(TestPlan testPlan) {
        User adminUser = new User();
        adminUser.setName(testPlan.getAdmin().getUsername());
        adminUser.setPassword(testPlan.getAdmin().getPassword());

        UserRoles adminRoles = new UserRoles();
        ArrayList<String> rolesList = new ArrayList<>();
        rolesList.add(testPlan.getAdmin().getRole());
        adminRoles.setRoleList(rolesList);
        adminUser.setRoles(adminRoles);

        userMap.put(adminUser.getName(), adminUser);
    }

    public void setupBeforeMigration() throws AndesEventAdminServiceEventAdminException,
                                              AndesAdminServiceBrokerManagerAdminException,
                                              RemoteException,
                                              RemoteUserStoreManagerServiceUserStoreExceptionException,
                                              LogoutAuthenticationExceptionException {
        createRoles();
        createUsers();
        setupBeforeMigrationQueuesTopics();
        loginClient.logOut();
    }

    public void verifyAfterMigration() throws RemoteUserStoreManagerServiceUserStoreExceptionException,
                                              RemoteException, AndesAdminServiceBrokerManagerAdminException {
        LOGGER.info("Verifying Roles");
        verifyRoles();
        LOGGER.info("Verifying Users");
        verifyUsers();
        LOGGER.info("Verifying Queues");
        verifyQueueMessageCount();
        LOGGER.info("Verifying Topics");
        verifyTopicSubscribers();
    }

    private void verifyTopicSubscribers() throws AndesAdminServiceBrokerManagerAdminException, RemoteException {

        List<Topic> topicList = testPlan.getTopics().getTopicList();
        for (Topic topic: topicList) {
            List<Publisher> publisherList = topic.getBeforeMigration().getPublishers().getPublisherList();
            long beforeMigrationPublishCount = getPublishedMessageCount(publisherList);
            long afterMigrationPublishCount = getPublishedMessageCount(
                    topic.getAfterMigration().getPublishers().getPublisherList());
            List<Subscriber> subscriberList = topic.getBeforeMigration().getSubscribers().getSubscriberList();
            verifySubscriberMessageCount( topic,
                                          beforeMigrationPublishCount + afterMigrationPublishCount,
                                          subscriberList);

            subscriberList = topic.getAfterMigration().getSubscribers().getSubscriberList();
            verifySubscriberMessageCount(topic, afterMigrationPublishCount, subscriberList);
        }
    }

    private void verifySubscriberMessageCount(Topic topic, long publishedMessageCount, List<Subscriber> subscriberList)
            throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        for (Subscriber subscriber: subscriberList) {
            long expectedMessageCount = publishedMessageCount - subscriber.getMessageCount();
            long messageCount = adminServiceWrapper.getPendingMessageCount(subscriber.getSubscriptionId());
            if (messageCount != expectedMessageCount) {
                LOGGER.error("Message count mismatch for subscription {} for topic {}",
                             subscriber.getSubscriptionId(), topic.getName());
            }
        }
    }

    private long getPublishedMessageCount(List<Publisher> publisherList) {
        long publishedCount = 0;
        for (Publisher publisher: publisherList) {
            publishedCount += publisher.getMessageCount();
        }
        return publishedCount;
    }

    private void verifyQueueMessageCount() throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        Map<String, org.wso2.carbon.andes.stub.admin.types.Queue> queuesOnServerMap = getQueuesOnServerMap();

        List<Queue> queueList = testPlan.getQueues().getQueueList();
        for (Queue queue: queueList) {
            if (!queuesOnServerMap.containsKey(queue.getName())) {
                LOGGER.error("Queue {} doesn't exist in MB", queue.getName());
            }

            org.wso2.carbon.andes.stub.admin.types.Queue serverQueue = queuesOnServerMap.get(queue.getName());
            long expectedMessageCount =
                    publishedMessageCount(queue.getBeforeMigration().getSenders().getSenderList())
                    + publishedMessageCount(queue.getAfterMigration().getSenders().getSenderList())
                    - consumedMessageCount(queue.getBeforeMigration().getReceivers().getReceiverList())
                    - (long) consumedMessageCount(queue.getAfterMigration().getReceivers().getReceiverList());

            if (serverQueue.getMessageCount() != expectedMessageCount) {
                LOGGER.error("Message count mismatch for queue: {}.", queue.getName());
            }
        }
    }

    private Map<String, org.wso2.carbon.andes.stub.admin.types.Queue> getQueuesOnServerMap()
            throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        org.wso2.carbon.andes.stub.admin.types.Queue[] queuesOnServer = adminServiceWrapper.getQueueList();
        Map<String, org.wso2.carbon.andes.stub.admin.types.Queue> queuesOnServerMap = new HashMap<>();
        for (org.wso2.carbon.andes.stub.admin.types.Queue queue: queuesOnServer) {
            queuesOnServerMap.put(queue.getQueueName(), queue);
        }
        return queuesOnServerMap;
    }

    private int consumedMessageCount(List<Receiver> receiverList) {
        int consumedCount = 0;
        for (Receiver publisher: receiverList) {
            consumedCount += publisher.getMessageCount();
        }
        return consumedCount;
    }

    private int publishedMessageCount(List<Sender> senderList) {
        int publishedCount = 0;
        for (Sender sender: senderList) {
            publishedCount += sender.getMessageCount();
        }
        return publishedCount;
    }

    private void verifyUsers() throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException {
        String[] users = adminServiceWrapper.listUsers();
        Set<String> usersInServer = new HashSet<>(Arrays.asList(users));

        List<User> userList = testPlan.getUsers().getUserList();
        for (User user: userList) {
            if (!usersInServer.contains(user.getName())) {
                LOGGER.error("User {} doesn't exist in MB", user.getName());
            }
        }
    }

    private void verifyRoles() throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException {
        String[] roles = adminServiceWrapper.listRoles();
        Set<String> rolesInServer = new HashSet<>(Arrays.asList(roles));

        List<Role> roleList = testPlan.getRoles().getRoleList();
        for(Role role: roleList) {
            if(!rolesInServer.contains(role.getName())) {
                LOGGER.error("Role {} doesn't exist in MB", role.getName());
            }
        }
    }

    public void setupAfterMigration() throws AndesEventAdminServiceEventAdminException,
                                             AndesAdminServiceBrokerManagerAdminException,
                                             RemoteException, LogoutAuthenticationExceptionException {
        setupAfterMigrationQueuesTopics();
        loginClient.logOut();
    }

    private void setupAfterMigrationQueuesTopics() throws AndesAdminServiceBrokerManagerAdminException,
                                                          RemoteException, AndesEventAdminServiceEventAdminException {

        setupAfterMigrationQueues(testPlan, adminServiceWrapper);
        setupAfterMigrationTopics(testPlan, adminServiceWrapper);
    }

    private void setupBeforeMigrationQueuesTopics() throws AndesEventAdminServiceEventAdminException, RemoteException,
                                                           AndesAdminServiceBrokerManagerAdminException {

        setupBeforeMigrationQueues(testPlan, adminServiceWrapper);
        setupBeforeMigrationTopics(testPlan, adminServiceWrapper);
    }

    private void setupBeforeMigrationQueues(TestPlan testPlan, AdminServiceWrapper adminServiceWrapper)
            throws AndesAdminServiceBrokerManagerAdminException, RemoteException {

        List<Queue> queueList = testPlan.getQueues().getQueueList();
        for(Queue queue: queueList) {
            Map<String, QueueRolePermission> roleMap = new HashMap<>();

            adminServiceWrapper.createQueue(queue.getName());

            List<Sender> senderList = queue.getBeforeMigration().getSenders().getSenderList();
            updateSenderRoleMap(roleMap, senderList);

            List<Receiver> receiverList = queue.getBeforeMigration().getReceivers().getReceiverList();
            updateReceiverRoleMap(roleMap, receiverList);

            for (QueueRolePermission queueRolePermission: roleMap.values()) {
                adminServiceWrapper.updateQueuePermissions(queue.getName(), queueRolePermission);
            }
        }
    }

    private void updateReceiverRoleMap(Map<String, QueueRolePermission> roleMap, List<Receiver> receiverList) {
        for (Receiver receiver: receiverList) {
            String user = receiver.getUser();
            User userObj = userMap.get(user);
            String roleName = userObj.getRoles().getRoleList().get(0);
            QueueRolePermission queueRolePermission = roleMap.computeIfAbsent(roleName, s -> new QueueRolePermission());
            queueRolePermission.setRoleName(roleName);
            queueRolePermission.setAllowedToConsume(true);
        }
    }

    private void updateSenderRoleMap(Map<String, QueueRolePermission> roleMap, List<Sender> senderList) {
        for (Sender sender: senderList) {
            String user = sender.getUser();
            User userObj = userMap.get(user);
            String roleName = userObj.getRoles().getRoleList().get(0);
            QueueRolePermission queueRolePermission = roleMap.computeIfAbsent(roleName, s -> new QueueRolePermission());
            queueRolePermission.setRoleName(roleName);
            queueRolePermission.setAllowedToPublish(true);
        }
    }

    private void setupBeforeMigrationTopics(TestPlan testPlan, AdminServiceWrapper adminServiceWrapper)
            throws AndesEventAdminServiceEventAdminException, RemoteException {
        List<Topic> topicList = testPlan.getTopics().getTopicList();
        for(Topic topic: topicList) {
            Map<String, TopicRolePermission> roleMap = new HashMap<>();

            adminServiceWrapper.createTopic(topic.getName());

            List<Publisher> publisherList = topic.getBeforeMigration().getPublishers().getPublisherList();
            updatePublisherRoleMap(roleMap, publisherList);

            List<Subscriber> subscriberList = topic.getBeforeMigration().getSubscribers().getSubscriberList();
            updateSubscriberRoleMap(roleMap, subscriberList);

            for (TopicRolePermission queueRolePermission: roleMap.values()) {
                adminServiceWrapper.updateTopicPermission(topic.getName(), queueRolePermission);
            }
        }
    }

    private void updateSubscriberRoleMap(Map<String, TopicRolePermission> roleMap, List<Subscriber> subscriberList) {
        for (Subscriber subscriber: subscriberList) {
            String user = subscriber.getUser();
            User userObj = userMap.get(user);
            String roleName = userObj.getRoles().getRoleList().get(0);
            TopicRolePermission topicRolePermission = roleMap.computeIfAbsent(roleName, s -> new TopicRolePermission());
            topicRolePermission.setRoleName(roleName);
            topicRolePermission.setAllowedToSubscribe(true);
        }
    }

    private void updatePublisherRoleMap(Map<String, TopicRolePermission> roleMap, List<Publisher> publisherList) {
        for (Publisher publisher: publisherList) {
            String user = publisher.getUser();
            User userObj = userMap.get(user);
            String roleName = userObj.getRoles().getRoleList().get(0);
            TopicRolePermission topicRolePermission = roleMap.computeIfAbsent(roleName, s -> new TopicRolePermission());
            topicRolePermission.setRoleName(roleName);
            topicRolePermission.setAllowedToPublish(true);
        }
    }

    private void setupAfterMigrationQueues(TestPlan testPlan, AdminServiceWrapper adminServiceWrapper)
            throws AndesAdminServiceBrokerManagerAdminException, RemoteException {

        List<Queue> queueList = testPlan.getQueues().getQueueList();
        for(Queue queue: queueList) {
            Map<String, QueueRolePermission> roleMap = new HashMap<>();

            List<Sender> senderList = queue.getAfterMigration().getSenders().getSenderList();
            updateSenderRoleMap(roleMap, senderList);

            List<Receiver> receiverList = queue.getAfterMigration().getReceivers().getReceiverList();
            updateReceiverRoleMap(roleMap, receiverList);

            for (QueueRolePermission queueRolePermission: roleMap.values()) {
                adminServiceWrapper.updateQueuePermissions(queue.getName(), queueRolePermission);
            }
        }
    }

    private void setupAfterMigrationTopics(TestPlan testPlan, AdminServiceWrapper adminServiceWrapper)
            throws RemoteException, AndesEventAdminServiceEventAdminException {

        List<Topic> topicList = testPlan.getTopics().getTopicList();
        for(Topic topic: topicList) {
            Map<String, TopicRolePermission> roleMap = new HashMap<>();

            List<Publisher> publisherList = topic.getAfterMigration().getPublishers().getPublisherList();
            updatePublisherRoleMap(roleMap, publisherList);

            List<Subscriber> subscriberList = topic.getAfterMigration().getSubscribers().getSubscriberList();
            updateSubscriberRoleMap(roleMap, subscriberList);

            for (TopicRolePermission queueRolePermission: roleMap.values()) {
                adminServiceWrapper.updateTopicPermission(topic.getName(), queueRolePermission);
            }
        }
    }

    private void createUsers() throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException {

        List<User> userList = testPlan.getUsers().getUserList();
        for (User user: userList) {
            userMap.put(user.getName(), user);
            List<String> userRoleList = user.getRoles().getRoleList();
            adminServiceWrapper.addUser(user.getName(), user.getPassword(),
                                        userRoleList.toArray(new String[0]));
        }
    }

    private void createRoles() throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException {
        List<Role> roleList = testPlan.getRoles().getRoleList();

        for (Role role: roleList) {
            adminServiceWrapper.addRole(role.getName(), role.getPermissions().getPermissionList());
        }
    }

    private AdminServiceWrapper login(String adminUser, String adminPassword, String backendUrl)
            throws RemoteException, LoginAuthenticationExceptionException, MalformedURLException {

        loginClient = new LoginClient(backendUrl);
        String sessionId = loginClient.authenticate(adminUser, adminPassword);
        return new
                AdminServiceWrapper(backendUrl, sessionId);
    }

    public void cleanUp()
            throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException,
                   AndesEventAdminServiceEventAdminException, AndesAdminServiceBrokerManagerAdminException {
        deleteUsers();
        deleteRoles();
        deleteQueues();
        deleteTopics();
    }

    private void deleteUsers() throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException {
        List<User> userList = testPlan.getUsers().getUserList();
        for (User user: userList) {
            adminServiceWrapper.removeUser(user.getName());
        }
    }

    private void deleteRoles() throws RemoteUserStoreManagerServiceUserStoreExceptionException, RemoteException {
        List<Role> roleList = testPlan.getRoles().getRoleList();
        for (Role role: roleList) {
            adminServiceWrapper.removeRole(role.getName());
        }
    }

    private void deleteQueues() throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        List<Queue> queueList = testPlan.getQueues().getQueueList();
        for (Queue queue: queueList) {
            adminServiceWrapper.deleteQueue(queue.getName());
        }
    }

    private void deleteTopics() throws AndesEventAdminServiceEventAdminException, RemoteException,
                                       AndesAdminServiceBrokerManagerAdminException {
        List<Topic> topicList = testPlan.getTopics().getTopicList();
        for (Topic topic: topicList) {
            removeSubscribers(topic, topic.getBeforeMigration().getSubscribers().getSubscriberList());
            removeSubscribers(topic, topic.getAfterMigration().getSubscribers().getSubscriberList());

            adminServiceWrapper.deleteTopic(topic.getName());
        }
    }

    private void removeSubscribers(Topic topic, List<Subscriber> subscriberList)
            throws AndesAdminServiceBrokerManagerAdminException, RemoteException {
        for (Subscriber subscriber: subscriberList) {
            adminServiceWrapper.removeSubscriber(subscriber.getSubscriptionId(), topic.getName());
        }
    }

    public Map<String, User> getUserMap() {
        if(!userMapPopulated) {
            List<User> userList = testPlan.getUsers().getUserList();

            for (User user: userList) {
                userMap.put(user.getName(), user);
            }
        }
        return userMap;
    }
}
