/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.security;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.rb.MessageCode;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Helper class for Hadoop credential provider functionality.
 */

public final class CredentialProviderHelper {

    private static final Logger LOG = LoggerFactory.getLogger(CredentialProviderHelper.class);
    public static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";

    private CredentialProviderHelper() {

    }

    public static String resolveAlias(Configuration conf, String alias) throws BeaconException {
        try {
            if (StringUtils.isBlank(conf.get(CREDENTIAL_PROVIDER_PATH))) {
                BeaconConfig config = BeaconConfig.getInstance();
                String credentialProviderPath = config.getEngine().getCredentialProviderPath();
                if (StringUtils.isBlank(credentialProviderPath)) {
                    throw new BeaconException(MessageCode.COMM_010008.name(), "The provided path");
                }
                conf.set(CREDENTIAL_PROVIDER_PATH, credentialProviderPath);
            }

            char[] cred = conf.getPassword(alias);
            if (cred == null) {
                throw new BeaconException(MessageCode.COMM_000002.name(), alias);
            }
            return new String(cred);

        } catch (IOException ioe) {
            throw new BeaconException(ioe);
        }
    }

    public static void createCredentialEntry(Configuration conf, String alias, String credential)
            throws BeaconException {
        try {
            List<CredentialProvider> result = CredentialProviderFactory.getProviders(conf);
            if (result == null || result.isEmpty()) {
                throw new BeaconException(MessageCode.COMM_000003.name());
            }
            CredentialProvider provider = result.get(0);
            if (provider == null) {
                throw new BeaconException(MessageCode.COMM_010008.name(), "CredentialProvider");
            }
            LOG.debug("Using credential provider {}", provider);

            provider.createCredentialEntry(alias, credential.toCharArray());
            provider.flush();
        } catch (IOException ioe) {
            throw new BeaconException(MessageCode.COMM_000004.name(), ioe);
        }
    }
}
