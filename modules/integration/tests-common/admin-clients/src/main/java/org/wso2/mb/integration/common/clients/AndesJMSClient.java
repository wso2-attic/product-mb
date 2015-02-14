package org.wso2.mb.integration.common.clients;

import org.wso2.mb.integration.common.clients.configurations.AndesJMSClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;

import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.Properties;

public abstract class AndesJMSClient{
    protected final AndesJMSClientConfiguration jmsConfig;

    private InitialContext initialContext;

    protected AndesJMSClient(AndesJMSClientConfiguration config) throws NamingException {
        this.jmsConfig = config;

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, AndesClientConstants.ANDES_ICF);
        properties.put(AndesClientConstants.CF_NAME_PREFIX + AndesClientConstants.CF_NAME, jmsConfig.getConnectionString());
        properties.put(jmsConfig.getExchangeType().getType() + "." + jmsConfig.getDestinationName(), jmsConfig.getDestinationName());

        initialContext = new InitialContext(properties);
    }

    protected InitialContext getInitialContext() {
        return initialContext;
    }

    public abstract void startClient() throws JMSException, NamingException, IOException;

    public abstract void stopClient() throws JMSException;

    public abstract AndesJMSClientConfiguration getConfig();
}
