package org.wso2.mb.integration.tests.server.mgt;


import org.apache.axis2.AxisFault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.authenticator.stub.LogoutAuthenticationExceptionException;
import org.wso2.carbon.integration.common.admin.client.AuthenticatorClient;
import java.rmi.RemoteException;

/**
 * A utility for logging into & logging out of Carbon servers
 */
public class LoginLogoutClient {
    private static final Log log = LogFactory.getLog(LoginLogoutClient.class);
    private String userName;
    private String password;

    private AuthenticatorClient loginClient;

    public LoginLogoutClient(String backendURL,String userName,String password) throws AxisFault {

        this.userName=userName;
        this.password = password;
        this.loginClient = new AuthenticatorClient(backendURL);
    }

    /**
     * Log in to a Carbon server
     *
     * @return The session cookie on successful login
     */
    public String login() throws RemoteException, LoginAuthenticationExceptionException {
        return loginClient.login(userName, password, "localhost");

    }

    /**
     * Log out from carbon server
     */
    public void logout() throws LogoutAuthenticationExceptionException, RemoteException {
        loginClient.logOut();
    }
}

