package com.hortonworks.beacon.util;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Engine;
import com.hortonworks.beacon.exceptions.BeaconException;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

public class SSLUtils {

    public static final HostnameVerifier HOSTNAME_VERIFIER =  new HostnameVerifier() {
        public boolean verify(String urlHostName, SSLSession session) {
            return session.getPeerHost().equals(urlHostName);
        }
    };


    public static SSLContext getSSLContext() throws BeaconException {
        try {
            BeaconConfig config = BeaconConfig.getInstance();

            Engine engine = config.getEngine();
            KeyManager[] kms = null;
            TrustManager[] tms = null;

            if (engine.isTlsEnabled()) {

                String keyStorePath = engine.getKeyStore();
                String keyStorePass = engine.resolveKeyStorePassword();

                if (keyStorePath != null && keyStorePass != null) {
                    KeyStore keyStore = KeyStore.getInstance("jks");
                    try (FileInputStream in = new FileInputStream(keyStorePath)) {
                        keyStore.load(in, keyStorePass.toCharArray());
                        KeyManagerFactory keyManagerFactory = KeyManagerFactory
                                .getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        keyManagerFactory.init(keyStore, keyStorePass.toCharArray());
                        kms = keyManagerFactory.getKeyManagers();
                    }
                }

                String trustStorePath = engine.getTrustStore();
                String trustStorePass = engine.resolveTrustStorePassword();

                if (trustStorePath != null && trustStorePass != null) {
                    KeyStore trustStore = KeyStore.getInstance("jks");
                    try (FileInputStream in = new FileInputStream(trustStorePath)) {
                        trustStore.load(in, trustStorePass.toCharArray());
                        TrustManagerFactory trustManagerFactory = TrustManagerFactory
                                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                        trustManagerFactory.init(trustStore);
                        tms = trustManagerFactory.getTrustManagers();
                    }
                }
            }
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kms, tms, new SecureRandom());
            return sslContext;
        } catch(Exception e) {
            throw new BeaconException("Unable to initialize SSL context", e);
        }
    }

}
