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

package com.hortonworks.beacon.security;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.config.BeaconConfig;
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
    public static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";

    private static final Logger LOG = LoggerFactory.getLogger(BeaconCredentialProvider.class);
    private final String credFile;
    private CredentialProvider credProvider;

    public BeaconCredentialProvider(String credentialFile) throws BeaconException {
        Configuration conf = new Configuration();
        conf.set(CREDENTIAL_PROVIDER_PATH, credentialFile);
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
