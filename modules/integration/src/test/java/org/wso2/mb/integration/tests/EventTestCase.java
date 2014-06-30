/**
* Copyright (c) 2009, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
* <p/>
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* <p/>
* http://www.apache.org/licenses/LICENSE-2.0
* <p/>
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.wso2.mb.integration.tests;

import junit.framework.Assert;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axis2.AxisFault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.event.client.broker.BrokerClient;
import org.wso2.carbon.event.client.broker.BrokerClientException;
import org.wso2.carbon.event.client.stub.generated.authentication.AuthenticationExceptionException;
import org.wso2.carbon.event.stub.internal.TopicManagerAdminServiceStub;
import org.wso2.carbon.integration.core.AuthenticateStub;
import org.wso2.carbon.integration.framework.ClientConnectionUtil;
import org.wso2.carbon.integration.framework.LoginLogoutUtil;

/**
 * Test case for adding a topic
 */
public class EventTestCase {

    private LoginLogoutUtil util = new LoginLogoutUtil();
    private TopicManagerAdminServiceStub topicManagerAdminServiceStub;
    private static final Log log = LogFactory.getLog(EventTestCase.class);
    private BrokerClient brokerClient;

    @BeforeClass(groups = {"wso2.mb"})
    public void login() throws Exception {
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            log.error("Error in thread sleep in EventTestCase ", e);
            Assert.fail("Error in thread sleep in EventTestCase");
        }
        ClientConnectionUtil.waitForPort(9443);
        String loggedInSessionCookie = util.login();
        topicManagerAdminServiceStub = new TopicManagerAdminServiceStub("https://localhost:9443/services/TopicManagerAdminService");
        AuthenticateStub authenticateStub = new AuthenticateStub();
        authenticateStub.authenticateAdminStub(topicManagerAdminServiceStub, loggedInSessionCookie);
        createBrokerClient();
    }

    @Test(groups = {"wso2.mb"})
    public void createTopicTest() throws Exception {
        String testTopic = "/root/topic/myTopic";
        topicManagerAdminServiceStub.addTopic(testTopic);
        log.info("Adding Topic" + testTopic);

    }
    @Test(groups = {"wso2.mb"})
    public void subscribeToTopicTest() throws BrokerClientException {
        String testTopic = "/root/topic/myTopic";
        String eventSinkURL =  "http://localhost:9763/services/EventSinkService/getOMElement";
        brokerClient.subscribe(testTopic,eventSinkURL);
        log.info("Subscribing to Topic" + testTopic);
    }

    @Test(groups = {"wso2.mb"})
    public void publishingToTopicTest() throws AxisFault {
        String testTopic = "/root/topic/myTopic";
        brokerClient.publish(testTopic,getOMElementToSend());
        log.info("Publishing message to Topic" + testTopic);
    }

    private void createBrokerClient() throws AxisFault, AuthenticationExceptionException {
          //create the broker client
        this.brokerClient = new BrokerClient("https://localhost:9443/services/EventBrokerService", "admin", "admin");
    }

    private OMElement getOMElementToSend() {
        OMFactory omFactory = OMAbstractFactory.getOMFactory();
        OMNamespace omNamespace = omFactory.createOMNamespace("http://ws.sample.org", "ns1");
        OMElement receiveElement = omFactory.createOMElement("receive", omNamespace);
        OMElement messageElement = omFactory.createOMElement("message", omNamespace);
        messageElement.setText("Test publish message");
        receiveElement.addChild(messageElement);
        return receiveElement;

    }


    @AfterClass(groups = {"wso2.mb"})
    public void logout() throws Exception {
        ClientConnectionUtil.waitForPort(9443);
        util.logout();
    }

}
