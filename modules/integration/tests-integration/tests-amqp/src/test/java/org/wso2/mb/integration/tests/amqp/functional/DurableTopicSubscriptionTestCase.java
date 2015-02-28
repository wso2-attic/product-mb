package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;

/**
 * This class holds set of test cases to verify if durable topic
 * subscriptions happen according to spec.
 */
public class DurableTopicSubscriptionTestCase {

    /**
     * Creating a client with a subscription ID and unSubscribe it and
     * create another client with the same subscription ID.
     *
     * @throws JMSException
     * @throws NamingException
     * @throws AndesClientConfigurationException
     * @throws IOException
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void basicSubscriptionTest()
            throws JMSException, NamingException, AndesClientConfigurationException, IOException {

        // Creating configurations
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "myTopic1");
        consumerConfig.setDurable(true, "durableSub1");

        // Creating clients
        AndesClient initialConsumerClient = new AndesClient(consumerConfig, true);
        initialConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        initialConsumerClient.getConsumers().get(0).unSubscribe(false);

        AndesClientUtils.sleepForInterval(2000L);

        AndesClient secondaryConsumerClient = new AndesClient(consumerConfig, true);
        secondaryConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        secondaryConsumerClient.getConsumers().get(0).unSubscribe(false);

        // Stopping the clients
        initialConsumerClient.stopClient();
        secondaryConsumerClient.stopClient();
    }


    /**
     * Create with sub id=x topic=y. Try another subscription with same params.
     * should rejects the subscription.
     *
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     * @throws AndesClientConfigurationException
     */
    @Test(groups = {"wso2.mb", "topic"}, expectedExceptions = javax.jms.JMSException.class, expectedExceptionsMessageRegExp = ".*Cannot subscribe to queue .* as it already has an existing exclusive consumer.*")
    public void multipleSubsWithSameIdTest()
            throws JMSException, NamingException, IOException, AndesClientConfigurationException {
        // Creating configurations
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "myTopic2");
        consumerConfig.setDurable(true, "sriLanka");

        // Creating clients
        AndesClient initialConsumerClient = new AndesClient(consumerConfig, true);
        initialConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        AndesClient secondaryConsumerClient = new AndesClient(consumerConfig, true);
        secondaryConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        // Stopping the clients
        initialConsumerClient.stopClient();
        secondaryConsumerClient.stopClient();

        AndesClientUtils.sleepForInterval(2000L);
    }

    /**
     * Create with sub id=x topic=y. Try another with sub id=z topic=y. Should be allowed.
     *
     * @throws JMSException
     * @throws NamingException
     * @throws AndesClientConfigurationException
     * @throws IOException
     * @throws CloneNotSupportedException
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void multipleSubsWithDifferentIdTest()
            throws JMSException, NamingException, AndesClientConfigurationException, IOException,
                   CloneNotSupportedException {

        // Creating configurations
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "myTopic3");
        consumerConfig.setDurable(true, "test1");

        AndesJMSConsumerClientConfiguration secondaryConsumerConfig = consumerConfig.clone();
        secondaryConsumerConfig.setSubscriptionID("test2");

        // Creating clients
        AndesClient initialConsumerClient = new AndesClient(consumerConfig, true);
        initialConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        AndesClient secondaryConsumerClient = new AndesClient(secondaryConsumerConfig, true);
        secondaryConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        // Stopping the clients
        initialConsumerClient.stopClient();
        secondaryConsumerClient.stopClient();

        AndesClientUtils.sleepForInterval(2000L);
    }

    /**
     * 1. Create with sub id= x topic=y.
     * 2. Close it.
     * 3. Then try with sub id= x topic=z.
     * 4. Should reject the subscription.
     *
     * @throws JMSException
     * @throws NamingException
     * @throws AndesClientConfigurationException
     * @throws IOException
     * @throws CloneNotSupportedException
     */
    @Test(groups = {"wso2.mb", "topic"}, expectedExceptions = javax.jms.JMSException.class, expectedExceptionsMessageRegExp = ".*An Exclusive Bindings already exists for different topic.*")
    public void multipleSubsToDifferentTopicsWithSameSubIdTest()
            throws JMSException, NamingException, AndesClientConfigurationException, IOException,
                   CloneNotSupportedException {

        // Creating configurations
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "myTopic4");
        consumerConfig.setDurable(true, "test3");

        // Creating clients
        AndesClient initialConsumerClient = new AndesClient(consumerConfig, true);
        initialConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        initialConsumerClient.stopClient();

        AndesJMSConsumerClientConfiguration secondConsumerConfig = consumerConfig.clone();
        secondConsumerConfig.setDestinationName("myTopic5");
        AndesClient secondaryConsumerClient = new AndesClient(secondConsumerConfig, true);
        secondaryConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        secondaryConsumerClient.stopClient();

        AndesClientUtils.sleepForInterval(2000L);
    }

    /**
     * 1. Create with sub id=x topic=y.
     * 2. Create a normal topic subscription topic=y.
     *
     * @throws JMSException
     * @throws NamingException
     * @throws AndesClientConfigurationException
     * @throws IOException
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void durableTopicWithNormalTopicTest()
            throws JMSException, NamingException, AndesClientConfigurationException, IOException {

        // Creating configurations
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "myTopic5");
        consumerConfig.setDurable(true, "test5");

        // Creating clients
        AndesClient initialConsumerClient = new AndesClient(consumerConfig, true);
        initialConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        AndesJMSConsumerClientConfiguration secondConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "myTopic5");
        AndesClient secondaryConsumerClient = new AndesClient(secondConsumerConfig, true);
        secondaryConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        // Stopping the clients
        initialConsumerClient.stopClient();
        secondaryConsumerClient.stopClient();

        AndesClientUtils.sleepForInterval(2000L);
    }

    /**
     * 1. Create with sub id=x topic=y.
     * 2. UnSubscribe.
     * 3. Now try sub id=z topic=y.
     *
     * @throws JMSException
     * @throws NamingException
     * @throws AndesClientConfigurationException
     * @throws CloneNotSupportedException
     * @throws IOException
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void multipleSubsWithDiffIDsToSameTopicTest()
            throws JMSException, NamingException, AndesClientConfigurationException,
                   CloneNotSupportedException, IOException {
        // Creating configurations
        AndesJMSConsumerClientConfiguration firstConsumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "multiSubTopic");
        firstConsumerConfig.setDurable(true, "new1");

        AndesJMSConsumerClientConfiguration secondConsumerConfig = firstConsumerConfig.clone();
        secondConsumerConfig.setSubscriptionID("new2");

        AndesJMSConsumerClientConfiguration thirdConsumerConfig = firstConsumerConfig.clone();
        thirdConsumerConfig.setSubscriptionID("new3");

        AndesJMSConsumerClientConfiguration forthConsumerConfig = firstConsumerConfig.clone();
        forthConsumerConfig.setSubscriptionID("new4");

        // Creating clients
        AndesClient firstConsumerClient = new AndesClient(firstConsumerConfig, true);
        firstConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        AndesClient secondConsumerClient = new AndesClient(secondConsumerConfig, true);
        secondConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        AndesClient thirdConsumerClient = new AndesClient(thirdConsumerConfig, true);
        thirdConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        AndesClient forthConsumerClient = new AndesClient(forthConsumerConfig, true);
        forthConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        // Stopping the clients
        firstConsumerClient.stopClient();
        secondConsumerClient.stopClient();
        thirdConsumerClient.stopClient();
        forthConsumerClient.stopClient();

        AndesClientUtils.sleepForInterval(2000L);
    }

    /**
     * 1. Create with sub id= x topic=y.
     * 2. UnSubscribe.
     * 3. Now try sub id= x topic=z
     *
     * @throws JMSException
     * @throws NamingException
     * @throws IOException
     * @throws AndesClientConfigurationException
     */
    @Test(groups = {"wso2.mb", "topic"})
    public void subscribeUnSubscribeAndTryDifferentTopicTest()
            throws JMSException, NamingException, IOException, AndesClientConfigurationException {

        // Creating configurations
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "myTopic8");
        consumerConfig.setDurable(true, "test8");

        AndesJMSConsumerClientConfiguration secondaryConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.TOPIC, "myTopic9");
        consumerConfig.setDurable(true, "test8");


        // Creating clients
        AndesClient initialConsumerClient = new AndesClient(consumerConfig, true);
        initialConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        initialConsumerClient.getConsumers().get(0).unSubscribe(false);

        AndesClientUtils.sleepForInterval(2000L);

        AndesClient secondaryConsumerClient = new AndesClient(secondaryConfig, true);
        secondaryConsumerClient.startClient();

        AndesClientUtils.sleepForInterval(2000L);

        secondaryConsumerClient.getConsumers().get(0).unSubscribe(false);

        // Stopping the clients
        initialConsumerClient.stopClient();
        secondaryConsumerClient.stopClient();

        AndesClientUtils.sleepForInterval(2000L);
    }
}
