/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.EncryptionAlgorithmType;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.security.BeaconCredentialProvider;
import org.apache.commons.lang3.StringUtils;
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
import java.util.Properties;

/**
 * Server side logic of cloud cred, extends client bean CloudCred.
 */
public class BeaconCloudCred extends CloudCred {
    private static final String CREDENTIAL_FILE_OWNER = "hive";
    private static final Logger LOG = LoggerFactory.getLogger(BeaconCloudCred.class);

    public BeaconCloudCred(CloudCred cloudCred) {
        super(cloudCred);
    }

    private void setOwnerForCredentialFile(String credentialFile) throws BeaconException {
        try {
            Path path = ProviderUtils.unnestUri(new URI(credentialFile));
            LOG.debug("Setting owner for {} to {}", path, CREDENTIAL_FILE_OWNER);
            FileSystem fs = path.getFileSystem(new Configuration());
            fs.setOwner(path, CREDENTIAL_FILE_OWNER, "hdfs");
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
                credProvider.createCredentialEntry(config.getHadoopConfigName(), configs.get(config));
                credUpdated = true;
            }
        }

        if (credUpdated) {
            credProvider.flush();
            setOwnerForCredentialFile(credentialFile);
        }
    }

    private String getHadoopCredentialPath() {
        String providerPath = BeaconConfig.getInstance().getEngine().getCloudCredProviderPath();
        providerPath = providerPath + getId() + BeaconConstants.JCEKS_EXT;
        return providerPath;
    }

    public Configuration getHadoopConf() {
        return getHadoopConf(true);
    }

    public Configuration getHadoopConf(boolean loadDefaults) {
        List<Config> requiredConfigs = getAuthType().getRequiredConfigs();
        Configuration conf = new Configuration(loadDefaults);
        boolean isCredentialRequired = false;   //Are there any stored passwords
        for (Config config : requiredConfigs) {
            if (config.getHadoopConfigName() != null) {
                if (config.isPassword()) {
                    isCredentialRequired = true;
                } else {
                    conf.set(config.getHadoopConfigName(), configs.get(config));
                }
            }
        }

        if (isCredentialRequired) {
            conf.set(BeaconConstants.CREDENTIAL_PROVIDER_PATH, getHadoopCredentialPath());
        }

        return conf;
    }

    public static Configuration getCloudEncryptionTypeConf(Properties properties) {
        Configuration conf = new Configuration(false);
        String cloudEncryptionAlgorithm = properties.getProperty(FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName());
        LOG.debug("Cloud encryption algorithm: {}", cloudEncryptionAlgorithm);
        if (StringUtils.isNotBlank(cloudEncryptionAlgorithm)) {
            try {
                EncryptionAlgorithmType encryptionAlgorithmType = EncryptionAlgorithmType.fromName(
                        cloudEncryptionAlgorithm);
                switch (encryptionAlgorithmType) {
                    case AWS_AES256:
                        conf.set(encryptionAlgorithmType.getConfName(), cloudEncryptionAlgorithm);
                        break;
                    case AWS_SSEKMS:
                        conf.set(encryptionAlgorithmType.getConfName(), cloudEncryptionAlgorithm);
                        String sseKmsKey = properties.getProperty(FSDRProperties.CLOUD_ENCRYPTIONKEY.getName());
                        if (StringUtils.isNotBlank(sseKmsKey)) {
                            conf.set(BeaconConstants.AWS_SSEKMSKEY, sseKmsKey);
                        }
                    default:
                        LOG.error("Encryption algorithm {} not found. Data encryption won't be enabled.",
                                cloudEncryptionAlgorithm);
                }
            } catch (IllegalArgumentException e) {
                LOG.error("Encryption algorithm {} not found. Data encryption won't be enabled.",
                        cloudEncryptionAlgorithm);
            }
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
        }
    }
}
