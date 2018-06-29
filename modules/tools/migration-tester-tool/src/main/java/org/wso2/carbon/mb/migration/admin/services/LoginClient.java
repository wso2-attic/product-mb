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
import org.apache.axis2.context.ServiceContext;
import org.apache.axis2.transport.http.HTTPConstants;
import org.wso2.carbon.authenticator.stub.AuthenticationAdminStub;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.authenticator.stub.LogoutAuthenticationExceptionException;

import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;

class LoginClient {
    private AuthenticationAdminStub authenticationAdminStub;

    private final URL backendUrl;

    LoginClient(String backEndUrl) throws AxisFault, MalformedURLException {
        String serviceName = "AuthenticationAdmin";
        String endPoint = backEndUrl + "/services/" + serviceName;
        this.backendUrl = new URL(backEndUrl);
        authenticationAdminStub = new AuthenticationAdminStub(endPoint);
    }

    String authenticate(String userName, String password) throws RemoteException,
                                                                 LoginAuthenticationExceptionException {

        String sessionCookie = null;

        if (authenticationAdminStub.login(userName, password, backendUrl.getHost())) {
            ServiceContext serviceContext =
                    authenticationAdminStub._getServiceClient().getLastOperationContext().getServiceContext();
            sessionCookie = (String) serviceContext.getProperty(HTTPConstants.COOKIE_STRING);
        }

        return sessionCookie;
    }

    void logOut() throws RemoteException, LogoutAuthenticationExceptionException {
        authenticationAdminStub.logout();
    }
}
