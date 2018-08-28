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

package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.util.CloudCredDao;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.security.BeaconCredentialProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Server side logic of cloud cred, extends client bean CloudCred.
 */
public class BeaconCloudCred extends CloudCred {
    private static final String CREDENTIAL_FILE_OWNER;

    static {
        CREDENTIAL_FILE_OWNER = System.getProperty("beacon.hive.username", "hive");
    }
    private static final Logger LOG = LoggerFactory.getLogger(BeaconCloudCred.class);

    public BeaconCloudCred(String cloudCredId) {
        this(new CloudCredDao().getCloudCred(cloudCredId));
    }

    public BeaconCloudCred(CloudCred cloudCred) {
        super(cloudCred);
    }

    private void setOwnerForCredentialFile(String credentialFile) throws BeaconException {
        try {
            Path path = ProviderUtils.unnestUri(new URI(credentialFile));
            LOG.debug("Setting owner for {} to {}", path, CREDENTIAL_FILE_OWNER);
            FileSystem fs = path.getFileSystem(new Configuration());
            fs.setOwner(path, CREDENTIAL_FILE_OWNER, "");
        } catch (URISyntaxException | IOException e) {
            throw new BeaconException(e);
        }
    }

    public void createCredential() throws BeaconException {
        String credentialFile = getHadoopCredentialPath();
        BeaconCredentialProvider credProvider = new BeaconCredentialProvider(credentialFile);
        boolean credUpdated = false;

        List<Config> requiredConfigs = getAuthType().getRequiredConfigs();
        for (Config config : requiredConfigs) {
            if (config.isPassword() && configs.containsKey(config)) {
                String hadoopConfig = getHadoopConfigName(config);
                credProvider.createCredentialEntry(hadoopConfig, configs.get(config));
                credUpdated = true;
            }
        }

        if (credUpdated) {
            credProvider.flush();
            setOwnerForCredentialFile(credentialFile);
        }
    }

    private String getHadoopConfigName(Config config) {
        String configName = config.getHadoopConfigName();
        for(Config cfg : getAuthType().getRequiredConfigs()) {
            configName = configName.replace("{"+ cfg.getName() + "}", configs.get(cfg));
        }
        return configName;
    }

    public String getHadoopCredentialPath() {
        String providerPath = BeaconConfig.getInstance().getEngine().getCloudCredProviderPath();
        providerPath = providerPath + getId() + BeaconConstants.JCEKS_EXT;
        return providerPath;
    }

    public Configuration getHadoopConf(boolean loadDefaults) {
        List<Config> requiredConfigs = getAuthType().getRequiredConfigs();
        Configuration conf = new Configuration(loadDefaults);
        //Disable filesystem caching for cloud connectors
        conf.set("fs." + getProvider().getHcfsScheme() + ".impl.disable.cache", "true");
        boolean isCredentialRequired = false;   //Are there any stored passwords
        for (Config config : requiredConfigs) {
            if (config.getHadoopConfigName() != null) {
                if (config.isPassword()) {
                    isCredentialRequired = true;
                } else {
                    conf.set(getHadoopConfigName(config), configs.get(config));
                }
            }
        }

        if (isCredentialRequired) {
            conf.set(BeaconConstants.CREDENTIAL_PROVIDER_PATH, getHadoopCredentialPath());
        }
        return conf;
    }

    public void removeHiddenConfigs() {
        Iterator<Map.Entry<Config, String>> iterator = configs.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<Config, String> entry = iterator.next();
            if (entry.getKey().isPassword()) {
                iterator.remove();
            }
        }
    }

    public void deleteCredential() throws BeaconException {
        try {
            Path path = ProviderUtils.unnestUri(new URI(getHadoopCredentialPath()));
            FileSystem fileSystem = FileSystem.get(new Configuration());
            LOG.debug("Deleting file {}", path);
            if (fileSystem.exists(path)) {
                boolean deleted = fileSystem.delete(path, false);
                if (!deleted) {
                    throw new BeaconException("Failed to delete credential file " + path);
                }
            }
        } catch (IOException | URISyntaxException e) {
            throw new BeaconException(e);
        }
    }

    public void updateCredential() throws BeaconException {
        String credentialFile = getHadoopCredentialPath();
        BeaconCredentialProvider credProvider = new BeaconCredentialProvider(credentialFile);
        boolean credUpdated = false;
        List<String> existAliases = credProvider.getAliases();

        List<Config> requiredConfigs = getAuthType().getRequiredConfigs();
        for (Config config : requiredConfigs) {
            if (config.isPassword() && configs.containsKey(config)) {
                String alias = config.getHadoopConfigName();
                if (existAliases.contains(alias)) {
                    //Update if the alias already exists
                    credProvider.updateCredentialEntry(alias, configs.get(config));
                    existAliases.remove(alias);
                } else {
                    //Create if the alias doesn't exist
                    credProvider.createCredentialEntry(alias, configs.get(config));
                }
                credUpdated = true;
            }
        }

        if (credUpdated) {
            credProvider.flush();
        }

        //Remove the extra aliases in the old credential
        if (!existAliases.isEmpty()) {
            for (String alias: existAliases) {
                credProvider.deleteCredentialEntry(alias);
                credUpdated = true;
            }
        }

        if (credUpdated) {
            credProvider.flush();
            setOwnerForCredentialFile(credentialFile);
        }
    }
}
