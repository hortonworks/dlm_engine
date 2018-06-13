/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */
package com.hortonworks.beacon.util;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Engine;
import com.hortonworks.beacon.exceptions.BeaconException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SSLUtils Class.
 */
public final class SSLUtils {

    public static final HostnameVerifier HOSTNAME_VERIFIER =  new HostnameVerifier() {
        public boolean verify(String urlHostName, SSLSession session) {
            return session.getPeerHost().equals(urlHostName);
        }
    };

    private SSLContext context = null;

    private static final Logger LOG = LoggerFactory.getLogger(SSLUtils.class);

    private SSLUtils() {
        try {
            initSSLContext();
        } catch (BeaconException be) {
            LOG.error("Unable to initialize SSL Context: " + be.getMessage(), be);
        }
    }
    private static final class Holder {
        private static final SSLUtils INSTANCE = new SSLUtils();
    }

    public static SSLContext getSSLContext() throws BeaconException {
        if (Holder.INSTANCE.context == null) {
            synchronized (SSLUtils.class) {
                Holder.INSTANCE.initSSLContext();
            }
        }
        return Holder.INSTANCE.context;
    }

    private void initSSLContext() throws BeaconException {
        try {
            BeaconConfig config = BeaconConfig.getInstance();

            Engine engine = config.getEngine();
            KeyManager[] kms = null;
            TrustManager[] tms = null;


            if (engine.isTlsEnabled()) {

                String keyStorePath = engine.getKeyStore();
                String keyStorePass = engine.resolveKeyStorePassword();
                String keyPass = engine.resolveKeyPassword();

                if (keyPass == null) {
                    keyPass = keyStorePass;

                }
                if (keyStorePath != null && keyStorePass != null) {
                    LOG.debug("SSLContext using keystore : {}", keyStorePath);

                    KeyStore keyStore = KeyStore.getInstance("jks");
                    try (FileInputStream in = new FileInputStream(keyStorePath)) {
                        keyStore.load(in, keyStorePass.toCharArray());
                        KeyManagerFactory keyManagerFactory = KeyManagerFactory
                                .getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        keyManagerFactory.init(keyStore, keyPass.toCharArray());
                        kms = keyManagerFactory.getKeyManagers();
                    }
                }

                String trustStorePath = engine.getTrustStore();
                String trustStorePass = engine.resolveTrustStorePassword();

                if (trustStorePath != null && trustStorePass != null) {
                    LOG.debug("SSLContext using truststore : {}", trustStorePath);

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
            context = sslContext;
        } catch(Throwable t) {
            LOG.error("Unable to initialize SSL context", t);
            throw new BeaconException("Unable to initialize SSL context", t);
        }
    }
}
