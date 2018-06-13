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

import com.hortonworks.beacon.EncryptionAlgorithmType;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.security.BeaconCredentialProvider;
import com.hortonworks.beacon.util.ReplicationType;
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
        conf.set(BeaconConstants.FS_S3A_IMPL_DISABLE_CACHE, "true");
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

    public Configuration getBucketEndpointConf(String path) throws BeaconException {
        Configuration conf = new Configuration(false);
        try {
            if (isBucketEndPointConfAvailable(path)) {
                return conf;
            }
            S3Operation s3Operation;
            switch (this.getAuthType()) {
                case AWS_ACCESSKEY:
                    String credentialProviderPath = getHadoopCredentialPath();
                    BeaconCredentialProvider beaconCredentialProvider = new BeaconCredentialProvider(
                            credentialProviderPath);
                    String accessKey = beaconCredentialProvider.resolveAlias(CloudCred.Config.AWS_ACCESS_KEY
                            .getHadoopConfigName());
                    String secretKey = beaconCredentialProvider.resolveAlias(CloudCred.Config.AWS_SECRET_KEY
                            .getHadoopConfigName());
                    s3Operation = new S3Operation(accessKey, secretKey);
                    break;
                case AWS_INSTANCEPROFILE:
                    s3Operation = new S3Operation();
                    break;
                default:
                    throw new BeaconException("AuthType {} not supported.", this.getAuthType());
            }
            String bucketName = new URI(path).getHost();
            String bucketEndPoint = s3Operation.getBucketEndPoint(bucketName);
            String bucketEndPointConfKey = getBucketEndpointConfKey(bucketName);
            LOG.debug("Path: {}, Conf Key: {} Bucket Endpoint: {}", path, bucketEndPointConfKey, bucketEndPoint);
            conf.set(bucketEndPointConfKey, bucketEndPoint);
            return conf;
        } catch (URISyntaxException e) {
            throw new BeaconException("Path not correct: {}", path, e);
        }
    }


    public Configuration getCloudEncryptionTypeConf(Properties properties, String cloudPath) throws BeaconException {
        Configuration conf = new Configuration(false);
        String cloudEncryptionAlgorithm = properties.getProperty(FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName());
        LOG.debug("Cloud encryption algorithm: {}", cloudEncryptionAlgorithm);
        String bucketName = null;
        try {
            bucketName = new URI(cloudPath).getHost();
        } catch (URISyntaxException e) {
            throw new BeaconException(e, "Unable to retrieve bucket name from cloud path {}", cloudPath);
        }
        if (StringUtils.isNotBlank(cloudEncryptionAlgorithm)) {
            try {
                EncryptionAlgorithmType encryptionAlgorithmType = EncryptionAlgorithmType.valueOf(
                        cloudEncryptionAlgorithm);
                switch (encryptionAlgorithmType) {
                    case AWS_SSES3:
                        String awsSSES3AlgoConfig = String.format(encryptionAlgorithmType.getConfName(), bucketName);
                        conf.set(awsSSES3AlgoConfig, encryptionAlgorithmType.getName());
                        break;
                    case AWS_SSEKMS:
                        String awsSSEKMSAlgoConfig = String.format(encryptionAlgorithmType.getConfName(), bucketName);
                        conf.set(awsSSEKMSAlgoConfig, encryptionAlgorithmType.getName());
                        String sseKmsKey = properties.getProperty(FSDRProperties.CLOUD_ENCRYPTIONKEY.getName());
                        if (StringUtils.isNotBlank(sseKmsKey)) {
                            String awsSSEKMSKeyConfig = String.format(BeaconConstants.AWS_SSEKMSKEY, bucketName);
                            conf.set(awsSSEKMSKeyConfig, sseKmsKey);
                        }
                        break;
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

    public Configuration getCloudEncryptionTypeConf(ReplicationPolicy policy, String cloudPath) throws BeaconException {
        Properties props = new Properties();
        boolean hiveClusterEncOn = false;
        Cluster targetCluster = null;
        boolean isHiveHCFSTarget = false;
        if (policy.getType().equalsIgnoreCase(ReplicationType.HIVE.getName())) {
            targetCluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
            isHiveHCFSTarget = PolicyHelper.isDatasetHCFS(targetCluster.getHiveWarehouseLocation());
        }
        // For a Hive target HCFS cluster, try getting enc details from Cluster, if absent, fall back to policy.
        if (isHiveHCFSTarget) {
            if (StringUtils.isNotBlank(targetCluster.getHiveCloudEncryptionAlgorithm())) {
                props.put(FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName(),
                          targetCluster.getHiveCloudEncryptionAlgorithm());
                hiveClusterEncOn = true;
            }
            if (StringUtils.isNotBlank(targetCluster.getHiveCloudEncryptionKey())) {
                props.put(FSDRProperties.CLOUD_ENCRYPTIONKEY.getName(), targetCluster.getHiveCloudEncryptionKey());
            }
        }
        if (!hiveClusterEncOn) {
            if (StringUtils.isNotBlank(policy.getCloudEncryptionAlgorithm())) {
                props.put(FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName(), policy.getCloudEncryptionAlgorithm());
            }
            if (StringUtils.isNotBlank(policy.getCloudEncryptionKey())) {
                props.put(FSDRProperties.CLOUD_ENCRYPTIONKEY.getName(), policy.getCloudEncryptionKey());
            }
        }
        return getCloudEncryptionTypeConf(props, cloudPath);
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

    private boolean isBucketEndPointConfAvailable(String path) throws URISyntaxException {
        Configuration defaultConf = new Configuration();
        String bucket = new URI(path).getHost();
        String bucketEndPointKey = getBucketEndpointConfKey(bucket);
        return StringUtils.isNotEmpty(defaultConf.getTrimmed(bucketEndPointKey));
    }

    private String getBucketEndpointConfKey(String bucket) {
        return String.format(BeaconConstants.AWS_BUCKET_ENDPOINT, bucket);
    }
}
