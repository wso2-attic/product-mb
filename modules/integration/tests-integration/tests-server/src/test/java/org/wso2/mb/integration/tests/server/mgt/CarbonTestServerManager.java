package org.wso2.mb.integration.tests.server.mgt;

import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.integration.common.extensions.carbonserver.TestServerManager;

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.Map;


public class CarbonTestServerManager extends TestServerManager {
    private String carbonHome;

    public CarbonTestServerManager(AutomationContext context) throws XPathExpressionException {
        super(context);
    }
    public CarbonTestServerManager(AutomationContext context, String carbonZip, Map<String, String> startupParameterMap)
            throws XPathExpressionException {
        super(context, carbonZip, startupParameterMap);
    }

    public CarbonTestServerManager(AutomationContext context, int portOffset) throws XPathExpressionException {
        super(context, portOffset);
    }

    public String startServer() throws Exception {
        carbonHome = super.startServer();
        return carbonHome;
    }

    public void stopServer() throws Exception {
        super.stopServer();
    }

    public String getCarbonHome() {
        return carbonHome;
    }

    protected void copyArtifacts(String carbonHome) throws IOException {
    }
}
