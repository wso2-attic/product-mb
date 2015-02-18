package org.wso2.mb.integration.common.clients.operations.topic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic Topic Subscriber to check the multi tenancy test cases
 */
public class TopicMessageSubscriber extends Thread {
    private static Log log = LogFactory.getLog(TopicMessageSubscriber.class);
    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "qpidConnectionfactory";
    private String userName = "admin";
    private String password = "admin";
    private static String CARBON_CLIENT_ID = "carbon";
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";
    private static String CARBON_DEFAULT_HOSTNAME = "localhost";
    private static String CARBON_DEFAULT_PORT = "5672";
    private String topicName = "tenant1.com/MYTopic";
    private TopicConnection topicConnection;
    private TopicSession topicSession;
    private TopicSubscriber topicSubscriber;
    private int messageCount;
    private int receivedMessageCount = 0;
    private long currentTime;

    /**
     * Constructs a Topic Subscriber
     * @param username username to authenticate
     * @param password password to authenticate
     * @param topicName topic name to subscribe
     * @param messageCount Number of messages
     */
    public TopicMessageSubscriber(String username, String password, String topicName, int messageCount, int runTime){
        this.userName = username;
        this.password = password;
        this.topicName = topicName;
        this.messageCount = messageCount;
    }

    /**
     * To subscribe to the topic
     * @throws NamingException
     * @throws JMSException
     */
    private void subscribe() throws NamingException, JMSException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        TopicConnectionFactory connFactory = (TopicConnectionFactory) ctx.lookup(CF_NAME);
        topicConnection = connFactory.createTopicConnection();
        topicConnection.start();
        topicSession =
                topicConnection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);

        Topic topic = topicSession.createTopic(topicName);

        topicSubscriber = topicSession.createSubscriber(topic);

    }

    public void run(){
        try {
            subscribe();
            receive();
        } catch (Exception e){
            log.error(e);
        }

    }

    /**
     * To receive the message
     * @throws NamingException
     * @throws JMSException
     */
    private void receive() throws NamingException, JMSException {

        for(receivedMessageCount = 0; receivedMessageCount <messageCount;){
            Message message = topicSubscriber.receive();
            if (message != null && message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                System.out.println("Message Received : " + textMessage.getText());
                receivedMessageCount++;
            }

        }

        // Housekeeping
        topicSubscriber.close();
        topicSession.close();
        topicConnection.stop();
        topicConnection.close();
    }

    private String getTCPConnectionURL(String username, String password) {
        // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'
        return new StringBuffer()
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(CARBON_CLIENT_ID)
                .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(CARBON_DEFAULT_HOSTNAME).append(":").append(CARBON_DEFAULT_PORT).append("'")
                .toString();
    }

    /**
     * To get the received message count
     * @return the received message count by the subscriber
     */
    public int getReceivedMessageCount(){
        return receivedMessageCount;
    }
}
