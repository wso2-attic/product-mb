package org.wso2.mb.integration.common.clients.operations.topic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public class BasicTopicSubscriber {

    private static Log log = LogFactory.getLog(BasicTopicSubscriber.class);

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "andesConnectionfactory";
    private static final String CARBON_CLIENT_ID = "carbon";
    private static final String CARBON_VIRTUAL_HOST_NAME = "carbon";

    private TopicConnection topicConnection = null;
    private TopicSession topicSession = null;
    private String subscriptionId = null;
    InitialContext ctx = null;
    private TopicSubscriber topicSubscriber = null;

    public BasicTopicSubscriber(String host, String port, String userName, String password,
                                String topicName)


            throws NamingException, JMSException {

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties
                .put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password, host, port));
        properties.put("topic." + topicName, topicName);

        log.info("getTCPConnectionURL(userName,password) = " + getTCPConnectionURL(userName,password, host,port));

        ctx = new InitialContext(properties);
        // Lookup connection factory
        TopicConnectionFactory connFactory = (TopicConnectionFactory) ctx.lookup(CF_NAME);
        topicConnection = connFactory.createTopicConnection();
        //topicConnection.setClientID(subscriptionId);
        topicConnection.start();
    }

    public void subscribe(String topicName, boolean isDurable, String subscriptionId)
            throws JMSException, NamingException {

        topicSession = topicConnection.createTopicSession(false, TopicSession.AUTO_ACKNOWLEDGE);

        Topic topic = (Topic) ctx.lookup(topicName);
        log.info("Starting listening on topic: " + topic);

        if (isDurable) {
            topicSubscriber = topicSession.createDurableSubscriber(topic, subscriptionId);
        } else {
            topicSubscriber = topicSession.createSubscriber(topic);
        }
    }

    public void close() {
        try {
            log.info("closing Subscriber");
            topicSubscriber.close();
            topicSession.close();
            topicConnection.close();
            log.info("done closing Subscriber");
        } catch (JMSException e) {
            log.error("Error stop listening.", e);
        }
    }

    public void unsubscribe(String subscriptionId) {
        try {
            log.info("unSubscribing Subscriber");
            this.topicSession.unsubscribe(subscriptionId);
            topicSubscriber.close();
            topicSession.close();
            topicConnection.close();
            log.info("done unSubscribing Subscriber");
        } catch (JMSException e) {
            log.error("Error in removing subscription.", e);
        }
    }

    private String getTCPConnectionURL(String username, String password, String hostName, String port) {
        return new StringBuffer()
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(CARBON_CLIENT_ID)
                .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(hostName).append(":").append(port).append("'")
                .toString();
    }
}
