/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.security;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import java.io.IOException;
import java.util.List;

/**
 * Helper class for Hadoop credential provider functionality.
 */

public final class CredentialProviderHelper {

    private static final BeaconLog LOG = BeaconLog.getLog(CredentialProviderHelper.class);
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
            LOG.debug("Using credential provider {0}", provider);

            provider.createCredentialEntry(alias, credential.toCharArray());
            provider.flush();
        } catch (IOException ioe) {
            throw new BeaconException(MessageCode.COMM_000004.name(), ioe);
        }
    }
}
