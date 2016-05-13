/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.carbon.andes.transports.mqtt;

import org.wso2.carbon.andes.transports.config.MqttSecuredTransportProperties;
import org.wso2.carbon.andes.transports.mqtt.ssl.SSLConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * Specifies the functions requires to be used during server initialization
 */
public class Util {
    private static final String protocol = "TLS";

    /**
     * Compose SSL context through the provided configuration
     *
     * @param mqttTransportProperties the properties which holds information related to composing the SSL context
     * @return SSLContext
     */
    public static SSLContext getSSLConfig(MqttSecuredTransportProperties mqttTransportProperties) {

        String certPass = mqttTransportProperties.getCertPass();
        String keyStorePass = mqttTransportProperties.getKeyStorePass();
        String keyStoreFile = mqttTransportProperties.getKeyStoreFile();
        String trustStoreFile = mqttTransportProperties.getTrustStoreFile();
        String trustStorePass = mqttTransportProperties.getTrustStorePass();

        if (certPass == null) {
            certPass = keyStorePass;
        }
        if (keyStoreFile == null || keyStorePass == null) {
            throw new IllegalArgumentException("keyStoreFile or keyStorePass not defined for " +
                    "HTTPS scheme");
        }
        File keyStore = new File(keyStoreFile);
        if (!keyStore.exists()) {
            throw new IllegalArgumentException("KeyStore File " + keyStoreFile + " not found");
        }
        SSLConfig sslConfig =
                new SSLConfig(keyStore, keyStorePass).setCertPass(certPass);
        if (trustStoreFile != null) {
            File trustStore = new File(trustStoreFile);
            if (!trustStore.exists()) {
                throw new IllegalArgumentException("trustStore File " + trustStoreFile + " not found");
            }
            if (trustStorePass == null) {
                throw new IllegalArgumentException("trustStorePass is not defined for HTTPS scheme");
            }
            sslConfig.setTrustStore(trustStore).setTrustStorePass(trustStorePass);
        }
        return getSSLContext(sslConfig);
    }

    /**
     * Get the SSLContext from the provided sslconfigration
     *
     * @param sslConfig configuration which holds ssl related information
     * @return SSLContext
     */
    private static SSLContext getSSLContext(SSLConfig sslConfig) {
        SSLContext serverContext;

        String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
        if (algorithm == null) {
            algorithm = "SunX509";
        }
        try {
            KeyStore ks = getKeyStore(sslConfig.getKeyStore(), sslConfig.getKeyStorePass());
            // Set up key manager factory to use our key store
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
            kmf.init(ks, sslConfig.getCertPass() != null ? sslConfig.getCertPass().toCharArray()
                    : sslConfig.getKeyStorePass().toCharArray());
            KeyManager[] keyManagers = kmf.getKeyManagers();
            TrustManager[] trustManagers = null;
            if (sslConfig.getTrustStore() != null) {
                KeyStore tks = getKeyStore(sslConfig.getTrustStore(), sslConfig.getTrustStorePass());
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
                tmf.init(tks);
                trustManagers = tmf.getTrustManagers();
            }
            serverContext = SSLContext.getInstance(protocol);
            serverContext.init(keyManagers, trustManagers, null);

            return serverContext;
        } catch (UnrecoverableKeyException | KeyManagementException |
                NoSuchAlgorithmException | KeyStoreException | IOException e) {
            throw new IllegalArgumentException("Failed to initialize the server-side SSLContext", e);
        }
    }

    /**
     * Gets the keystore to create SSL connection
     *
     * @param keyStore         keystore which holds the ssl related information
     * @param keyStorePassword keystore password which holds ssl related information
     * @return KeyStore
     * @throws IOException
     */
    private static KeyStore getKeyStore(File keyStore, String keyStorePassword) throws IOException {
        KeyStore ks;
        try (InputStream is = new FileInputStream(keyStore)) {
            ks = KeyStore.getInstance("JKS");
            ks.load(is, keyStorePassword.toCharArray());
        } catch (CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
            throw new IOException(e);
        }
        return ks;
    }
}
