/*
 * Copyright (c) 2005-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.mb.integration.tests.amqp.functional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.andes.stub.admin.types.QueueRolePermission;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.authenticator.stub.LogoutAuthenticationExceptionException;
import org.wso2.carbon.automation.engine.FrameworkConstants;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.carbon.integration.common.admin.client.UserManagementClient;
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;
import org.wso2.carbon.user.mgt.stub.UserAdminUserAdminException;
import org.wso2.carbon.user.mgt.stub.types.carbon.FlaggedName;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.AndesClientTemp;
import org.wso2.mb.integration.common.clients.AndesJMSClient;
import org.wso2.mb.integration.common.clients.AndesJMSPublisherClient;
import org.wso2.mb.integration.common.clients.AndesJMSSubscriberClient;
import org.wso2.mb.integration.common.clients.configurations.AndesClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSSubscriberClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.queue.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtilsTemp;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;
import org.xml.sax.SAXException;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.rmi.RemoteException;

/**
 * This class contains the test cases related to user authorization and queues
 */
public class QueueUserAuthorizationTestCase extends MBIntegrationBaseTest {
    private static final Logger log = LoggerFactory.getLogger(QueueUserAuthorizationTestCase.class);

    private static final String ADD_QUEUE_PERMISSION = "/permission/admin/manage/queue/addQueue";

    private static final String CREATE_PUB_SUB_QUEUE_ROLE = "create_pub_sub_queue_role";
    private static final String PUB_SUB_QUEUE_ROLE = "pub_sub_queue_role";
    private static final String NO_PERMISSION_QUEUE_ROLE = "no_permission_queue_role";
    private static final String QUEUE_PREFIX = "Q_";

    private static final int SEND_COUNT = 1;
    private static final int EXPECTED_COUNT = 1;
    private static final int RUNTIME = 20;

    private UserManagementClient userManagementClient;

    /**
     * Initializes before a test method. Removes users of admin group if exists. Adds new roles with permissions.
     *
     * @throws Exception
     */
    @BeforeMethod(alwaysRun = true)
    public void initialize() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_ADMIN);

        String[] CREATE_PUB_SUB_USERS = new String[]{"authUser1", "authUser2"};
        String[] PUB_SUB_USERS = new String[]{"authUser3", "authUser4"};
        String[] NO_PERMISSION_USERS = new String[]{"authUser5"};
        String[] ALL_USERS = new String[]{"authUser1", "authUser2", "authUser3", "authUser4", "authUser5"};

        userManagementClient = new UserManagementClient(backendURL, "admin", "admin");

        // removing admin permission for all users
        userManagementClient.updateUserListOfRole(FrameworkConstants.ADMIN_ROLE, null, ALL_USERS);

        // adding roles along with users
        userManagementClient.addRole(CREATE_PUB_SUB_QUEUE_ROLE, CREATE_PUB_SUB_USERS, new String[]{ADD_QUEUE_PERMISSION});
        userManagementClient.addRole(PUB_SUB_QUEUE_ROLE, PUB_SUB_USERS, new String[]{});
        userManagementClient.addRole(NO_PERMISSION_QUEUE_ROLE, NO_PERMISSION_USERS, new String[]{});
    }

    /**
     * Cleans up the test case effects. Created roles and internal queue related roles are created.
     *
     * @throws java.rmi.RemoteException
     * @throws UserAdminUserAdminException
     */
    @AfterMethod(alwaysRun = true)
    public void cleanUp() throws RemoteException, UserAdminUserAdminException {
        // delete roles
        userManagementClient.deleteRole(CREATE_PUB_SUB_QUEUE_ROLE);
        userManagementClient.deleteRole(PUB_SUB_QUEUE_ROLE);
        userManagementClient.deleteRole(NO_PERMISSION_QUEUE_ROLE);

        // delete roles specific to queues
        FlaggedName[] allRoles = userManagementClient.getAllRolesNames("*", 10);
        for (FlaggedName allRole : allRoles) {
            if (allRole.getItemName().contains(QUEUE_PREFIX)) {
                userManagementClient.deleteRole(allRole.getItemName());
            }
        }
    }

    /**
     * User creates a queue and then publishes and consumes messages.
     *
     * @throws java.rmi.RemoteException
     * @throws UserAdminUserAdminException
     * @throws javax.xml.xpath.XPathExpressionException
     */
    @Test(groups = {"wso2.mb", "queue"})
    public void performQueuePermissionTestCase()
            throws IOException, UserAdminUserAdminException, XPathExpressionException,
                   JMSException, NamingException {


        try {
            Thread s = new Thread(new AndesJMSSubscriberClient());

            Thread p = new Thread(new AndesJMSPublisherClient());

            s.start();
//            Thread.sleep(30000);
            p.start();

            Thread.sleep(30000);

        } catch (Exception e) {
            log.error("ERROR AT TEST");
            e.printStackTrace();
        }

//        AndesJMSPublisherClient q = new AndesJMSPublisherClient();

////        AndesClientTemp receivingAndesClientForAuthUs = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:testQueue1",
////                                                                            "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
////                                                                            "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser1", "authUser1");
////        receivingAndesClientForAuthUs.startWorking();
//
//        // creating, publish and subscribe using "authUser1"
//        AndesJMSSubscriberClientConfiguration andesJMSSubscriberClientConfiguration = new AndesJMSSubscriberClientConfiguration.AndesJMSSubscriberClientConfigurationBuilder("admin", "admin", "127.0.0.1", 5672, ExchangeType.QUEUE, "q1").setAcknowledgeMode(1).setMaximumMessagesToReceived(5).build();
////        andesSubscriberClientConfiguration.setHostName("127.0.0.1");
////        andesSubscriberClientConfiguration.setPort(5672);
////        andesSubscriberClientConfiguration.setUserName("admin");
////        andesSubscriberClientConfiguration.setPassword("admin");
////        andesSubscriberClientConfiguration.setExchangeType(ExchangeType.QUEUE);
////        andesSubscriberClientConfiguration.setDestinationName("queue1");
////        andesSubscriberClientConfiguration.setAcknowledgeMode(1);
////        andesSubscriberClientConfiguration.setPrintsPerMessageCount(-1);
//
//        AndesClient receivingAndesClientForAuthUser1 = new AndesJMSSubscriberClient(andesJMSSubscriberClientConfiguration);
//        receivingAndesClientForAuthUser1.startClient();
//
////        AndesJMSClientConfiguration temp = (AndesJMSClientConfiguration) receivingAndesClientForAuthUser1.getConfig();
////        AndesJMSPublisherClientConfiguration t = temp;
//
//        AndesJMSPublisherClientConfiguration andesPublisherClientConfiguration = new AndesJMSPublisherClientConfiguration.AndesJMSPublisherClientConfigurationBuilder(andesJMSSubscriberClientConfiguration).setNumberOfMessagesToSend(5).build();
////        andesPublisherClientConfiguration.setHostName("127.0.0.1");
////        andesPublisherClientConfiguration.setPort(5672);
////        andesPublisherClientConfiguration.setUserName("authUser1");
////        andesPublisherClientConfiguration.setPassword("authUser1");
////        andesPublisherClientConfiguration.setExchangeType(ExchangeType.QUEUE);
////        andesPublisherClientConfiguration.setDestinationName("queue1");
////        andesPublisherClientConfiguration.setNumberOfMessagesToSend(10);
////        andesPublisherClientConfiguration.setPrintsPerMessageCount(-1);
////        andesPublisherClientConfiguration.setJMSMessageType(JMSMessageType.TEXT);
////        andesPublisherClientConfiguration.setMessageLifetime(0);
//        AndesClient sendingAndesClientForAuthUser1 = new AndesJMSPublisherClient(andesPublisherClientConfiguration);
//        sendingAndesClientForAuthUser1.startClient();
//
//        log.info("STATS : " + receivingAndesClientForAuthUser1.getPublisherTPS() + " " +
//                 receivingAndesClientForAuthUser1.getSubscriberTPS() + " " +
//                 receivingAndesClientForAuthUser1.getAverageLatency());
//
//        log.info("STATS : " + sendingAndesClientForAuthUser1.getPublisherTPS() + " " +
//                 sendingAndesClientForAuthUser1.getSubscriberTPS() + " " +
//                 sendingAndesClientForAuthUser1.getAverageLatency());


//        AndesClientTemp sendingAndesClientForAuthUser1 = new AndesClientTemp("send", "127.0.0.1:5672", "queue:testQueue1", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser1", "authUser1");
//        sendingAndesClientForAuthUser1.startWorking();
//
//        boolean receiveSuccessForAuthUser1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser1,
//                                                                                           EXPECTED_COUNT, RUNTIME);
//        boolean sendSuccessForAuthUser1 = AndesClientUtils.getIfSenderIsSuccess(sendingAndesClientForAuthUser1, SEND_COUNT);
//
//        Assert.assertTrue(receiveSuccessForAuthUser1, "Failed receiving messages for authUser1");
//        Assert.assertTrue(sendSuccessForAuthUser1, "Failed sending messages for authUser1");
    }

//    /**
//     * User1 and User2 exists in the same role where create queue permission is assigned.
//     * User1 creates a queue and then publishes and consumes messages. User2 tries to publish and consume messages. But unable to succeed.
//     *
//     * @throws RemoteException
//     * @throws UserAdminUserAdminException
//     * @throws XPathExpressionException
//     */
//    @Test(groups = {"wso2.mb", "queue"}, expectedExceptions = {JMSException.class}, expectedExceptionsMessageRegExp = ".*Permission denied.*")
//    public void performQueuePermissionSameRoleUsersWithNoPublishOrConsume()
//            throws RemoteException, UserAdminUserAdminException, XPathExpressionException,
//                   JMSException {
//        // creating, publish and subscribe using "authUser1"
//        AndesClient receivingAndesClientForAuthUser1 = new AndesClient("receive", "127.0.0.1:5672", "queue:testQueue2",
//                                                                       "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                       "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser1", "authUser1");
//        receivingAndesClientForAuthUser1.startWorking();
//        AndesClient sendingAndesClientForAuthUser1 = new AndesClient("send", "127.0.0.1:5672", "queue:testQueue2", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser1", "authUser1");
//        sendingAndesClientForAuthUser1.startWorking();
//
//        boolean receiveSuccessForAuthUser1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser1,
//                                                                                               EXPECTED_COUNT, RUNTIME);
//        boolean sendSuccessForAuthUser1 = AndesClientUtils.getIfSenderIsSuccess(sendingAndesClientForAuthUser1, SEND_COUNT);
//
//        Assert.assertTrue(receiveSuccessForAuthUser1, "Failed receiving messages for authUser1");
//        Assert.assertTrue(sendSuccessForAuthUser1, "Failed sending messages for authUser1");
//
//        // publishing and creating using "authUser2" to "testQueue2"
//        AndesClient receivingAndesClientForAuthUser2 = new AndesClient("receive", "127.0.0.1:5672", "queue:testQueue2",
//                                                                       "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                       "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser2", "authUser2");
//        // a JMS exception will occur when "startWorking" is called. But the exception is logged and not thrown.
//        receivingAndesClientForAuthUser2.startWorking();
//        AndesClient sendingAndesClientForAuthUser2 = new AndesClient("send", "127.0.0.1:5672", "queue:testQueue2", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser2", "authUser2");
//        sendingAndesClientForAuthUser2.startWorking();
//
//        // a NullPointerException is thrown here
//        AndesClientUtils.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser2, EXPECTED_COUNT, RUNTIME);
//        AndesClientUtils.getIfSenderIsSuccess(sendingAndesClientForAuthUser2, SEND_COUNT);
//    }
//
//    /**
//     * User1 and User2 exists in the same role where create queue permission is assigned.
//     * User1 creates a queue and then publishes and consumes messages. Add publish and consume permissions to the role in which User1 exists.
//     * User2 tries to publish and consume messages. User2 succeeds.
//     *
//     * @throws RemoteException
//     * @throws UserAdminUserAdminException
//     * @throws XPathExpressionException
//     */
//    @Test(groups = {"wso2.mb", "queue"})
//    public void performQueuePermissionSameRoleUsersWithPublishOrConsume()
//            throws IOException, LoginAuthenticationExceptionException, URISyntaxException,
//                   LogoutAuthenticationExceptionException, XMLStreamException,
//                   AndesAdminServiceBrokerManagerAdminException, SAXException,
//                   XPathExpressionException, UserAdminUserAdminException, JMSException {
//        // creating, publish and subscribe using "authUser1"
//        AndesClientTemp receivingAndesClientForAuthUser1 = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:testQueue3",
//                                                                       "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                       "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser1", "authUser1");
//        receivingAndesClientForAuthUser1.startWorking();
//        AndesClientTemp sendingAndesClientForAuthUser1 = new AndesClientTemp("send", "127.0.0.1:5672", "queue:testQueue3", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser1", "authUser1");
//        sendingAndesClientForAuthUser1.startWorking();
//
//        boolean receiveSuccessForAuthUser1 = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser1,
//                                                                                               EXPECTED_COUNT, RUNTIME);
//        boolean sendSuccessForAuthUser1 = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingAndesClientForAuthUser1, SEND_COUNT);
//
//        Assert.assertTrue(receiveSuccessForAuthUser1, "Failed receiving messages for authUser1");
//        Assert.assertTrue(sendSuccessForAuthUser1, "Failed sending messages for authUser1");
//
//        // adding "authUser2" to "testQueue3"
//        QueueRolePermission queueRolePermission = new QueueRolePermission();
//        queueRolePermission.setRoleName(CREATE_PUB_SUB_QUEUE_ROLE);
//        queueRolePermission.setAllowedToConsume(true);
//        queueRolePermission.setAllowedToPublish(true);
//        this.updateQueueRoleConsumePublishPermission("testQueue3", queueRolePermission);
//        log.info("Consume/Publish permissions updated for " + CREATE_PUB_SUB_QUEUE_ROLE);
//
//        // publishing and creating using "authUser2" to "testQueue3"
//        AndesClientTemp receivingAndesClientForAuthUser2 = new AndesClientTemp("receive", "127.0.0.1:5672", "queue:testQueue3",
//                                                                       "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                       "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser2", "authUser2");
//        receivingAndesClientForAuthUser2.startWorking();
//        AndesClientTemp sendingAndesClientForAuthUser2 = new AndesClientTemp("send", "127.0.0.1:5672", "queue:testQueue3", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser2", "authUser2");
//        sendingAndesClientForAuthUser2.startWorking();
//
//        boolean receiveSuccessForAuthUser2 = AndesClientUtilsTemp.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser2,
//                                                                                               EXPECTED_COUNT, RUNTIME);
//        boolean sendSuccessForAuthUser2 = AndesClientUtilsTemp.getIfSenderIsSuccess(sendingAndesClientForAuthUser2, SEND_COUNT);
//
//        Assert.assertTrue(receiveSuccessForAuthUser2, "Failed receiving messages for authUser2");
//        Assert.assertTrue(sendSuccessForAuthUser2, "Failed sending messages for authUser2");
//    }
//
//    /**
//     * User1 is in Role1 where there is queue creating permissions.
//     * User5 is in Role2 where there are no create queue permissions.
//     * User1 creates a queue and then publishes and consumes messages.
//     * User5 tries to publish and consume messages. User5 fails.
//     *
//     * @throws JMSException
//     */
//    @Test(groups = {"wso2.mb", "queue"}, expectedExceptions = {JMSException.class}, expectedExceptionsMessageRegExp = ".*Permission denied.*")
//    public void performQueuePermissionDifferentRoleUsersWithNoPermissions()
//            throws JMSException {
//        // creating, publish and subscribe using "authUser1"
//        AndesClient receivingAndesClientForAuthUser1 = new AndesClient("receive", "127.0.0.1:5672", "queue:testQueue4",
//                                                                       "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                       "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser1", "authUser1");
//        receivingAndesClientForAuthUser1.startWorking();
//        AndesClient sendingAndesClientForAuthUser1 = new AndesClient("send", "127.0.0.1:5672", "queue:testQueue4", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser1", "authUser1");
//        sendingAndesClientForAuthUser1.startWorking();
//
//        boolean receiveSuccessForAuthUser1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser1,
//                                                                                               EXPECTED_COUNT, RUNTIME);
//        boolean sendSuccessForAuthUser1 = AndesClientUtils.getIfSenderIsSuccess(sendingAndesClientForAuthUser1, SEND_COUNT);
//
//        Assert.assertTrue(receiveSuccessForAuthUser1, "Failed receiving messages for authUser1");
//        Assert.assertTrue(sendSuccessForAuthUser1, "Failed sending messages for authUser1");
//
//        // publishing and creating using "authUser5" to "testQueue4"
//        AndesClient receivingAndesClientForAuthUser5 = new AndesClient("receive", "127.0.0.1:5672", "queue:testQueue4",
//                                                                       "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                       "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser5", "authUser5");
//        receivingAndesClientForAuthUser5.startWorking();
//        AndesClient sendingAndesClientForAuthUser5 = new AndesClient("send", "127.0.0.1:5672", "queue:testQueue4", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser5", "authUser5");
//        sendingAndesClientForAuthUser5.startWorking();
//
//        AndesClientUtils.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser5,
//                                                          EXPECTED_COUNT, RUNTIME);
//        AndesClientUtils.getIfSenderIsSuccess(sendingAndesClientForAuthUser5, SEND_COUNT);
//    }
//
//    /**
//     * User1 exists in a role where create queue permission is assigned.
//     * User1 creates a queue and then publishes and consumes messages.
//     * User1 is removed from the role.
//     * User1 tries to publish and consume messages. User1 fails.
//     *
//     * @throws RemoteException
//     * @throws UserAdminUserAdminException
//     */
//    @Test(groups = {"wso2.mb", "queue"}, expectedExceptions = {JMSException.class}, expectedExceptionsMessageRegExp = ".*Permission denied.*")
//    public void performQueuePermissionSameUserRemovedFromRole()
//            throws RemoteException, UserAdminUserAdminException, JMSException {
//        // creating, publish and subscribe using "authUser1"
//        AndesClient receivingAndesClientForAuthUser1 = new AndesClient("receive", "127.0.0.1:5672", "queue:testQueue5",
//                                                                       "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                       "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser1", "authUser1");
//        receivingAndesClientForAuthUser1.startWorking();
//        AndesClient sendingAndesClientForAuthUser1 = new AndesClient("send", "127.0.0.1:5672", "queue:testQueue5", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser1", "authUser1");
//        sendingAndesClientForAuthUser1.startWorking();
//
//        // Removing authUser1 from create_pub_sub_queue_role and Internal/_testQueue5
//        userManagementClient.addRemoveRolesOfUser("authUser1", new String[]{NO_PERMISSION_QUEUE_ROLE}, new String[]{CREATE_PUB_SUB_QUEUE_ROLE, "Internal/Q_testQueue5"});
//        log.info("Removing authUser1 from " + CREATE_PUB_SUB_QUEUE_ROLE + " and Internal/Q_testQueue5");
//
//        receivingAndesClientForAuthUser1.startWorking();
//        sendingAndesClientForAuthUser1.startWorking();
//
//        AndesClientUtils.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser1,
//                                                          EXPECTED_COUNT, RUNTIME);
//        AndesClientUtils.getIfSenderIsSuccess(sendingAndesClientForAuthUser1, SEND_COUNT);
//    }
//
//    /**
//     * User1 and User2 exists in the same role where create queue permission is assigned.
//     * User1 creates a queue and then publishes and consumes messages.
//     * Admin assigns publishing and consuming  permissions to the role in which User1 and User2 are in.
//     * User1 is removed from the role.
//     * User2 tries to publish and consume messages. User2 succeeds.
//     *
//     * @throws RemoteException
//     * @throws UserAdminUserAdminException
//     * @throws XPathExpressionException
//     */
//    @Test(groups = {"wso2.mb", "queue"})
//    public void performQueuePermissionSameRoleAssignedPermissions()
//            throws IOException, LoginAuthenticationExceptionException, URISyntaxException,
//                   LogoutAuthenticationExceptionException, XMLStreamException,
//                   AndesAdminServiceBrokerManagerAdminException, SAXException,
//                   XPathExpressionException, UserAdminUserAdminException, JMSException {
//        // creating, publish and subscribe using "authUser1"
//        AndesClient receivingAndesClientForAuthUser1 = new AndesClient("receive", "127.0.0.1:5672", "queue:testQueue6",
//                                                                       "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                       "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser1", "authUser1");
//        receivingAndesClientForAuthUser1.startWorking();
//        AndesClient sendingAndesClientForAuthUser1 = new AndesClient("send", "127.0.0.1:5672", "queue:testQueue6", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser1", "authUser1");
//        sendingAndesClientForAuthUser1.startWorking();
//
//        boolean receiveSuccessForAuthUser1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser1,
//                                                                                               EXPECTED_COUNT, RUNTIME);
//        boolean sendSuccessForAuthUser1 = AndesClientUtils.getIfSenderIsSuccess(sendingAndesClientForAuthUser1, SEND_COUNT);
//
//        Assert.assertTrue(receiveSuccessForAuthUser1, "Failed receiving messages for authUser1");
//        Assert.assertTrue(sendSuccessForAuthUser1, "Failed sending messages for authUser1");
//
//        // Updating permissions for create_pub_sub_queue_role
//        QueueRolePermission queueRolePermission = new QueueRolePermission();
//        queueRolePermission.setRoleName(CREATE_PUB_SUB_QUEUE_ROLE);
//        queueRolePermission.setAllowedToConsume(true);
//        queueRolePermission.setAllowedToPublish(true);
//        this.updateQueueRoleConsumePublishPermission("testQueue6", queueRolePermission);
//        log.info("Consume/Publish permissions updated for " + CREATE_PUB_SUB_QUEUE_ROLE);
//
//        // Removing authUser1 permissions
//        userManagementClient.addRemoveRolesOfUser("authUser1", new String[]{NO_PERMISSION_QUEUE_ROLE}, new String[]{CREATE_PUB_SUB_QUEUE_ROLE, "Internal/Q_testQueue6"});
//        log.info("Removing authUser1 from " + CREATE_PUB_SUB_QUEUE_ROLE + " and Internal/Q_testQueue6");
//
//        // publishing and creating using "authUser2" to "testQueue6"
//        AndesClient receivingAndesClientForAuthUser2 = new AndesClient("receive", "127.0.0.1:5672", "queue:testQueue6",
//                                                                       "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                       "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser2", "authUser2");
//        receivingAndesClientForAuthUser2.startWorking();
//
//        AndesClient sendingAndesClientForAuthUser2 = new AndesClient("send", "127.0.0.1:5672", "queue:testQueue6", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser2", "authUser2");
//        sendingAndesClientForAuthUser2.startWorking();
//
//        boolean receiveSuccessForAuthUser2 = AndesClientUtils.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser2,
//                                                                                               EXPECTED_COUNT, RUNTIME);
//        boolean sendSuccessForAuthUser2 = AndesClientUtils.getIfSenderIsSuccess(sendingAndesClientForAuthUser2, SEND_COUNT);
//
//        Assert.assertTrue(receiveSuccessForAuthUser2, "Failed receiving messages for authUser2");
//        Assert.assertTrue(sendSuccessForAuthUser2, "Failed sending messages for authUser2");
//    }
//
//    /**
//     * User1 is in Role1 where there is queue creating permissions.
//     * User3 is in Role2 where there are no create queue permissions.
//     * User1 creates a queue and then publishes and consumes messages.
//     * Admin assigns publishing and consuming permissions to Role2.
//     * User3 is removed from Role1.
//     * User3 tries to publish and consume messages. User3 succeeds.
//     *
//     * @throws RemoteException
//     * @throws UserAdminUserAdminException
//     * @throws XPathExpressionException
//     */
//    @Test(groups = {"wso2.mb", "queue"})
//    public void performQueuePermissionDifferentRolesAssignedPermissions()
//            throws IOException, XPathExpressionException,
//                   AndesAdminServiceBrokerManagerAdminException, URISyntaxException, SAXException,
//                   XMLStreamException, UserAdminUserAdminException,
//                   LoginAuthenticationExceptionException, LogoutAuthenticationExceptionException,
//                   JMSException {
//        // creating, publish and subscribe using "admin"
//        AndesClient receivingAndesClientForAdmin = new AndesClient("receive", "127.0.0.1:5672", "queue:testQueue7",
//                                                                   "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                   "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "admin", "admin");
//        receivingAndesClientForAdmin.startWorking();
//        AndesClient sendingAndesClientForAdmin = new AndesClient("send", "127.0.0.1:5672", "queue:testQueue7", "100", "false",
//                                                                 Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                 "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "admin", "admin");
//        sendingAndesClientForAdmin.startWorking();
//
//        boolean receiveSuccessForAuthUser1 = AndesClientUtils.waitUntilMessagesAreReceived(receivingAndesClientForAdmin,
//                                                                                               EXPECTED_COUNT, RUNTIME);
//        boolean sendSuccessForAuthUser1 = AndesClientUtils.getIfSenderIsSuccess(sendingAndesClientForAdmin, SEND_COUNT);
//
//        Assert.assertTrue(receiveSuccessForAuthUser1, "Failed receiving messages for authUser1");
//        Assert.assertTrue(sendSuccessForAuthUser1, "Failed sending messages for authUser1");
//
//        // Updating permissions for pub_sub_queue_role
//        QueueRolePermission queueRolePermission = new QueueRolePermission();
//        queueRolePermission.setRoleName(PUB_SUB_QUEUE_ROLE);
//        queueRolePermission.setAllowedToConsume(true);
//        queueRolePermission.setAllowedToPublish(true);
//        this.updateQueueRoleConsumePublishPermission("testQueue7", queueRolePermission);
//        log.info("Consume/Publish permissions updated for " + PUB_SUB_QUEUE_ROLE);
//
//        // publishing and creating using "authUser3" to "testQueue7"
//        AndesClient receivingAndesClientForAuthUser3 = new AndesClient("receive", "127.0.0.1:5672", "queue:testQueue7",
//                                                                       "100", "false", Integer.toString(RUNTIME), Integer.toString(EXPECTED_COUNT),
//                                                                       "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + EXPECTED_COUNT, "", "authUser3", "authUser3");
//        receivingAndesClientForAuthUser3.startWorking();
//        AndesClient sendingAndesClientForAuthUser3 = new AndesClient("send", "127.0.0.1:5672", "queue:testQueue7", "100", "false",
//                                                                     Integer.toString(RUNTIME), Integer.toString(SEND_COUNT), "1",
//                                                                     "ackMode=1,delayBetweenMsg=0,stopAfter=" + SEND_COUNT, "", "authUser3", "authUser3");
//        sendingAndesClientForAuthUser3.startWorking();
//
//        boolean receiveSuccessForAuthUser3 = AndesClientUtils.waitUntilMessagesAreReceived(receivingAndesClientForAuthUser3,
//                                                                                               EXPECTED_COUNT, RUNTIME);
//        boolean sendSuccessForAuthUser3 = AndesClientUtils.getIfSenderIsSuccess(sendingAndesClientForAuthUser3, SEND_COUNT);
//
//        Assert.assertTrue(receiveSuccessForAuthUser3, "Failed receiving messages for authUser3");
//        Assert.assertTrue(sendSuccessForAuthUser3, "Failed sending messages for authUser3");
//    }

    /**
     * Assigning consuming publishing permissions of a queue to a role.
     *
     * @param queueName   the queue name
     * @param permissions new permissions for the role. can be publish, consume.
     * @throws javax.xml.xpath.XPathExpressionException
     * @throws java.io.IOException
     * @throws java.net.URISyntaxException
     * @throws org.xml.sax.SAXException
     * @throws javax.xml.stream.XMLStreamException
     * @throws org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException
     * @throws org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException
     * @throws org.wso2.carbon.authenticator.stub.LogoutAuthenticationExceptionException
     * @throws UserAdminUserAdminException
     */
    public void updateQueueRoleConsumePublishPermission(String queueName,
                                                        QueueRolePermission permissions)
            throws XPathExpressionException, IOException, URISyntaxException, SAXException,
                   XMLStreamException, LoginAuthenticationExceptionException,
                   AndesAdminServiceBrokerManagerAdminException,
                   LogoutAuthenticationExceptionException,
                   UserAdminUserAdminException {

        LoginLogoutClient loginLogoutClientForAdmin = new LoginLogoutClient(automationContext);
        String sessionCookie = loginLogoutClientForAdmin.login();
        AndesAdminClient andesAdminClient = new AndesAdminClient(backendURL, sessionCookie, ConfigurationContextProvider.getInstance().getConfigurationContext());
        andesAdminClient.updatePermissionForQueue(queueName, permissions);
        loginLogoutClientForAdmin.logout();
    }
}
