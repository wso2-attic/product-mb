/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.mb.integration.common.clients.operations.queue;

import org.apache.axis2.AxisFault;
import org.apache.axis2.client.Options;
import org.apache.axis2.client.ServiceClient;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.andes.stub.AndesAdminServiceStub;
import org.wso2.carbon.andes.stub.admin.types.Queue;

import java.rmi.RemoteException;


public class AndesAdminClient {

    private static final Log log = LogFactory.getLog(AndesAdminClient.class);

    String backendUrl = null;
    String SessionCookie = null;
    ConfigurationContext configurationContext = null;
    AndesAdminServiceStub stub = null;

    public AndesAdminClient(String backendUrl, String sessionCookie,
                            ConfigurationContext configurationContext) throws AxisFault {

        this.backendUrl = backendUrl
                + "AndesAdminService";
        this.SessionCookie = sessionCookie;
        this.configurationContext = configurationContext;
        stub = new AndesAdminServiceStub(configurationContext,
                this.backendUrl);
        configureCookie(stub._getServiceClient());

    }

    public void createQueue(String queue) throws Exception {
        stub.createQueue(queue);
    }

    public void deleteQueue(String queue) throws Exception {
        stub.deleteQueue(queue);
    }

    public Queue[] getAllQueues() throws RemoteException, AndesAdminServiceBrokerManagerAdminException {
        return stub.getAllQueues();
    }

    public Queue getQueueByName(String name) throws RemoteException, AndesAdminServiceBrokerManagerAdminException {
        Queue[] queues = stub.getAllQueues();

        if (queues != null && queues.length > 0) {
            for (Queue queue : queues) {
                if (queue.getQueueName().equalsIgnoreCase(name)) {
                    return queue;

                }
            }
        }

        return null;
    }

    private void configureCookie(ServiceClient client) throws AxisFault {
        if(SessionCookie != null){
            Options option = client.getOptions();
            option.setManageSession(true);
            option.setProperty(org.apache.axis2.transport.http.HTTPConstants.COOKIE_STRING,
                    SessionCookie);
        }
    }
}
