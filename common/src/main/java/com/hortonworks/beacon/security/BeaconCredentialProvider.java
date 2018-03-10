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

public final class BeaconCredentialProvider {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconCredentialProvider.class);
    private final String credFile;
    private CredentialProvider credProvider;

    public BeaconCredentialProvider(String credentialFile) throws BeaconException {
        Configuration conf = new Configuration();
        conf.set(BeaconConstants.CREDENTIAL_PROVIDER_PATH, credentialFile);
        try {
            List<CredentialProvider> result = CredentialProviderFactory.getProviders(conf);
            credProvider = result.get(0);
            this.credFile = credentialFile;
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    public BeaconCredentialProvider() throws BeaconException {
        this(BeaconConfig.getInstance().getEngine().getCredentialProviderPath());
    }

    public String resolveAlias(String alias) throws BeaconException {
        CredentialProvider.CredentialEntry credEntry = null;
        try {
            credEntry = credProvider.getCredentialEntry(alias);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
        if (credEntry == null) {
            throw new BeaconException("Can't find the alias " + alias);
        }
        return String.valueOf(credEntry.getCredential());
    }

    public void createCredentialEntry(String alias, String value) throws BeaconException {
        try {
            credProvider.createCredentialEntry(alias, value.toCharArray());
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    public void updateCredentialEntry(String alias, String value) throws BeaconException {
        try {
            credProvider.deleteCredentialEntry(alias);
            credProvider.createCredentialEntry(alias, value.toCharArray());
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    public void flush() throws BeaconException {
        if (credProvider != null) {
            try {
                LOG.info("Persisting credential file {} with aliases {}", credFile, credProvider.getAliases());
                credProvider.flush();
            } catch (IOException e) {
                throw new BeaconException(e);
            }
        }
    }

    public List<String> getAliases() throws BeaconException {
        try {
            return credProvider.getAliases();
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    public void deleteCredentialEntry(String alias) throws BeaconException {
        try {
            credProvider.deleteCredentialEntry(alias);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }
}
