package org.wso2.mb.integration.common.utils.backend;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;

/**
 * Created by pranavan on 2/16/15.
 */
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

}
