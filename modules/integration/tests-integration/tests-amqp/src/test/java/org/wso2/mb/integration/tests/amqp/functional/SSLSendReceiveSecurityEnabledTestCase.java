package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.utils.backend.MBSecurityManagerBaseTest;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import static org.testng.Assert.assertTrue;


public class SSLSendReceiveSecurityEnabledTestCase extends MBSecurityManagerBaseTest {

    private static int SSL_CONNECTION_PORT_NUMBER = 8672;

    @BeforeClass
    public void prepare() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue", "security"})
    public void performSingleQueueSendReceiveTestCase() {
        Integer sendCount = 100;
        Integer runTime = 20;
        Integer expectedCount = 100;
        String keyStorePath = System.getProperty("carbon.home") + File.separator + "repository" + File.separator
                + "resources" + File.separator + "security" + File.separator + "wso2carbon.jks";
        String trustStorePath = System.getProperty("carbon.home") + File.separator + "repository" + File.separator
                + "resources" + File.separator + "security" + File.separator + "client-truststore.jks";
        String keyStorePassword = "wso2carbon";
        String trustStorePassword = "wso2carbon";
        String sslConnectionURL = "amqp://admin:admin@carbon/carbon?brokerlist='tcp://localhost:8672?ssl='true'" +
                "&ssl_cert_alias='RootCA'&trust_store='" + trustStorePath + "'&trust_store_password='" +
                trustStorePassword
                + "'&key_store='" + keyStorePath + "'&key_store_password='" + keyStorePassword + "''";

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:8672", "queue:SSLSingleQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter=" + expectedCount, sslConnectionURL);

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:SSLSingleQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter=" + sendCount, sslConnectionURL);

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);
        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient, sendCount);

        assertTrue(sendSuccess, "Message sending failed.");
        assertTrue(receiveSuccess, "Message receiving failed.");
    }

    @Test(groups = {"wso2.mb", "queue", "security"})
    public void performCheckConnection() {

        String keyStorePath = System.getProperty("carbon.home") + File.separator + "repository" + File.separator
                + "resources" + File.separator + "security" + File.separator + "wso2carbon.jks";
        String trustStorePath = System.getProperty("carbon.home") + File.separator + "repository" + File.separator
                + "resources" + File.separator + "security" + File.separator + "client-truststore.jks";
        String keyStorePassword = "wso2carbon";
        String trustStorePassword = "wso2carbon";
        String sslConnectionURL = "amqp://admin:admin@carbon/carbon?brokerlist='tcp://localhost:8672?ssl='true'" +
                "&ssl_cert_alias='RootCA'&trust_store='" + trustStorePath + "'&trust_store_password='" +
                trustStorePassword
                + "'&key_store='" + keyStorePath + "'&key_store_password='" + keyStorePassword + "''";

        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        /**
         * Install all the all-trusting trusting manager
         */
        log.info("I am here");
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (GeneralSecurityException e) {
            log.error("GeneralSecurityException");

        }
        // Now you can access an https URL without having the certificate in the truststore
        try {
            URL url = new URL(sslConnectionURL);
        } catch (MalformedURLException e) {
            log.error("MalformedURLException");
        }

        log.info("I am here Finally");
    }

    /**
     * Throws a SecurityException if the
     * calling thread is not allowed to wait for a connection request on
     * the specified local port number.
     * If port is not 0, this method calls
     * checkPermission with the
     *
     * @throws SecurityException if the calling thread does not have
     *                           permission to listen on the specified port.
     */
    @Test(groups = {"wso2.mb", "queue", "security"})
    public void perfromPortConnection() {
        checkListen(SSL_CONNECTION_PORT_NUMBER);
    }
}
