package org.wso2.mb.integration.common.clients;

import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.configurations.AndesClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public abstract class AndesJMSClient extends AndesClient {
    private InitialContext initialContext;

    protected AndesJMSClient(AndesClientConfiguration config) throws NamingException {
        super(config);
        this.initialize();
    }

    protected void initialize() throws NamingException {
        super.initialize();
        AndesJMSClientConfiguration jmsConfig = (AndesJMSClientConfiguration) super.config;

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, AndesClientConstants.ANDES_ICF);
        properties.put(AndesClientConstants.CF_NAME_PREFIX + AndesClientConstants.CF_NAME, jmsConfig.getConnectionString());
        properties.put(jmsConfig.getExchangeType().getType() + "." + jmsConfig.getDestinationName(), jmsConfig.getDestinationName());

        initialContext = new InitialContext(properties);
    }

    public InitialContext getInitialContext() {
        return initialContext;
    }
}
