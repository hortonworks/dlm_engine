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
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
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

    private CredentialProviderHelper() {

    }

    public static String resolveAlias(Configuration conf, String alias) throws BeaconException {
        try {
            checkProviderConfig(conf);
            char[] cred = conf.getPassword(alias);
            if (cred == null) {
                throw new BeaconException("The provided alias {} cannot be resolved", alias);
            }
            return new String(cred);

        } catch (IOException ioe) {
            throw new BeaconException(ioe);
        }
    }

    public static void createCredentialEntry(Configuration conf, String alias, String credential)
            throws BeaconException {
        try {
            CredentialProvider provider = getCredentialProvider(conf);
            provider.createCredentialEntry(alias, credential.toCharArray());
            provider.flush();
        } catch (IOException ioe) {
            throw new BeaconException("Error while creating credential entry using the credential provider", ioe);
        }
    }

    public static void updateCredentialEntry(Configuration conf, String alias, String credential)
            throws BeaconException {
        try {
            deleteCredentialEntry(conf, alias);
            CredentialProvider provider = getCredentialProvider(conf);
            provider.createCredentialEntry(alias, credential.toCharArray());
            provider.flush();
        } catch (Exception e) {
            throw new BeaconException("Exception while updating credential.", e);
        }
    }

    public static void deleteCredentialEntry(Configuration conf, String alias) throws BeaconException {
        try {
            CredentialProvider provider = getCredentialProvider(conf);
            provider.deleteCredentialEntry(alias);
            provider.flush();
        } catch (Exception e) {
            throw new BeaconException("Exception while deleting credential.", e);
        }
    }

    private static CredentialProvider getCredentialProvider(Configuration conf) throws IOException, BeaconException {
        checkProviderConfig(conf);
        List<CredentialProvider> result = CredentialProviderFactory.getProviders(conf);
        if (result == null || result.isEmpty()) {
            throw new BeaconException("The provided configuration cannot be resolved");
        }
        CredentialProvider provider = result.get(0);
        if (provider == null) {
            throw new BeaconException("CredentialProvider cannot be null or empty");
        }
        LOG.debug("Using credential provider {}", provider);
        return provider;
    }

    private static void checkProviderConfig(Configuration conf) throws BeaconException {
        if (StringUtils.isBlank(conf.get(BeaconConstants.CREDENTIAL_PROVIDER_PATH))) {
            BeaconConfig config = BeaconConfig.getInstance();
            String credentialProviderPath = config.getEngine().getCredentialProviderPath();
            if (StringUtils.isBlank(credentialProviderPath)) {
                throw new BeaconException("CREDENTIAL_PROVIDER_PATH cannot be null or empty");
            }
            conf.set(BeaconConstants.CREDENTIAL_PROVIDER_PATH, credentialProviderPath);
        }
    }
}
