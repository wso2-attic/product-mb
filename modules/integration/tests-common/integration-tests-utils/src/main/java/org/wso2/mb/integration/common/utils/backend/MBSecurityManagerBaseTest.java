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

package org.wso2.mb.integration.common.utils.backend;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.DataProvider;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.engine.frameworkutils.FrameworkPathUtil;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;

import java.io.File;
import java.io.FileDescriptor;
import java.net.InetAddress;
import java.security.Permission;

public class MBSecurityManagerBaseTest extends SecurityManager {
    protected Log log = LogFactory.getLog(MBSecurityManagerBaseTest.class);
    protected AutomationContext automationContext;
    protected String backendURL;
    protected ServerConfigurationManager serverManager = null;

    /**
     * Initialize the base test by initializing the automation context.
     *
     * @param userMode The testing user mode
     * @throws Exception
     */
    protected void init(TestUserMode userMode) throws Exception {
        automationContext = new AutomationContext("MB", userMode);
        backendURL = automationContext.getContextUrls().getBackEndUrl();
    }

    /**
     * Restart the testing MB server in In-Memory H2 database mode by applying In-Memory database configurations
     * in andes-virtualhosts-H2-mem.xml file.
     *
     * @throws Exception
     */
    protected void restartServerWithH2MemMode() throws Exception {
        serverManager = new ServerConfigurationManager(automationContext);

        // Replace the broker.xml with the new configuration and restarts the server.
        serverManager.applyConfiguration(new File(FrameworkPathUtil.getSystemResourceLocation() + File.separator +
                        "artifacts" + File.separator + "mb" + File.separator + "config" + File.separator +
                        "broker.xml"),
                new File(ServerConfigurationManager.getCarbonHome() +
                        File.separator + "repository" + File.separator + "conf" + File.separator + "broker.xml"),
                true, true);
    }

    /**
     * Gracefully restart the current server which was deployed by the test suit. This can be used when a large
     * amount or large size of messages are tested to clean up the server before or after the test.
     *
     * @throws Exception
     */
    protected void restartServer() throws Exception {
        serverManager = new ServerConfigurationManager(automationContext);

        serverManager.restartGracefully();
    }

    @DataProvider
    public static Object[][] userModeDataProvider() {
        return new Object[][]{
                new Object[]{TestUserMode.SUPER_TENANT_ADMIN},
                new Object[]{TestUserMode.TENANT_ADMIN},
        };
    }

    @Override
    protected Class[] getClassContext() {
        return super.getClassContext();
    }

    @Override
    public void checkPermission(Permission perm) {
        super.checkPermission(perm);
    }

    @Override
    public void checkCreateClassLoader() {
        super.checkCreateClassLoader();
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        super.checkPermission(perm, context);
    }

    @Override
    public void checkAccess(Thread t) {
        super.checkAccess(t);
    }

    @Override
    public void checkAccess(ThreadGroup g) {
        super.checkAccess(g);
    }

    @Override
    public void checkExit(int status) {
        super.checkExit(status);
    }

    @Override
    public void checkExec(String cmd) {
        super.checkExec(cmd);
    }

    @Override
    public void checkLink(String lib) {
        super.checkLink(lib);
    }

    @Override
    public void checkRead(FileDescriptor fd) {
        super.checkRead(fd);
    }

    @Override
    public void checkRead(String file) {
        super.checkRead(file);
    }

    @Override
    public void checkRead(String file, Object context) {
        super.checkRead(file, context);
    }

    @Override
    public void checkWrite(FileDescriptor fd) {
        super.checkWrite(fd);
    }

    @Override
    public void checkWrite(String file) {
        super.checkWrite(file);
    }

    @Override
    public void checkDelete(String file) {
        super.checkDelete(file);
    }

    @Override
    public void checkConnect(String host, int port) {

        super.checkConnect(host, port);
    }

    @Override
    public void checkConnect(String host, int port, Object context) {
        super.checkConnect(host, port, context);
    }

    @Override
    public void checkListen(int port) {
        super.checkListen(port);
    }

    @Override
    public void checkAccept(String host, int port) {
        super.checkAccept(host, port);
    }

    @Override
    public void checkMulticast(InetAddress maddr) {
        super.checkMulticast(maddr);
    }

    @Override
    public void checkMulticast(InetAddress maddr, byte ttl) {
        super.checkMulticast(maddr, ttl);
    }

    @Override
    public void checkPropertiesAccess() {
        super.checkPropertiesAccess();
    }

    @Override
    public void checkPropertyAccess(String key) {
        super.checkPropertyAccess(key);
    }

    @Override
    public boolean checkTopLevelWindow(Object window) {
        return super.checkTopLevelWindow(window);
    }

    @Override
    public void checkPrintJobAccess() {
        super.checkPrintJobAccess();
    }

    @Override
    public void checkSystemClipboardAccess() {
        super.checkSystemClipboardAccess();
    }

    @Override
    public void checkAwtEventQueueAccess() {
        super.checkAwtEventQueueAccess();
    }

    @Override
    public void checkPackageAccess(String pkg) {
        super.checkPackageAccess(pkg);
    }

    @Override
    public void checkPackageDefinition(String pkg) {
        super.checkPackageDefinition(pkg);
    }

    @Override
    public void checkSetFactory() {
        super.checkSetFactory();
    }

    @Override
    public void checkMemberAccess(Class<?> clazz, int which) {
        super.checkMemberAccess(clazz, which);
    }

    @Override
    public void checkSecurityAccess(String target) {
        super.checkSecurityAccess(target);
    }

    @Override
    public ThreadGroup getThreadGroup() {
        return super.getThreadGroup();
    }
}
