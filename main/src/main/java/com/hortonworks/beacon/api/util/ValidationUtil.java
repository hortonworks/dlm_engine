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


package com.hortonworks.beacon.api.util;

import com.hortonworks.beacon.Destination;
import com.hortonworks.beacon.api.PropertiesIgnoreCase;
import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.entity.ClusterValidator;
import com.hortonworks.beacon.entity.EncryptionAlgorithmType;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.ReplicationPolicyProperties;
import com.hortonworks.beacon.entity.entityNeo.DataSet;
import com.hortonworks.beacon.entity.entityNeo.S3FSDataSet;
import com.hortonworks.beacon.entity.entityNeo.WASBFSDataSet;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.CloudCredDao;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.EncryptionZoneListing;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.notification.BeaconNotification;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.replication.fs.FSPolicyHelper;
import com.hortonworks.beacon.replication.fs.FSSnapshotUtils;
import com.hortonworks.beacon.replication.fs.SnapshotListing;
import com.hortonworks.beacon.replication.hive.HivePolicyHelper;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.hortonworks.beacon.client.entity.ReplicationPolicy.ReplicationPolicyFields.CLOUD_ENCRYPTIONALGORITHM;
import static com.hortonworks.beacon.client.entity.ReplicationPolicy.ReplicationPolicyFields.CLOUD_ENCRYPTIONKEY;

/**
 * Utility class to validate API requests.
 */
public final class ValidationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationUtil.class);
    private static ClusterDao clusterDao = new ClusterDao();
    private static CloudCredDao cloudCredDao = new CloudCredDao();


    private ValidationUtil() {
    }

    public static void validateClusterPairing(Cluster localCluster, Cluster remoteCluster) throws BeaconException {
        Properties localCustomProps = localCluster.getCustomProperties();
        Properties remoteCustomProps = remoteCluster.getCustomProperties();
        ClusterValidator clusterValidator = new ClusterValidator();
        clusterValidator.validateClusterInfo(remoteCluster, false);
        if (ClusterHelper.isHDFSEnabled(localCluster)
                != ClusterHelper.isHDFSEnabled(remoteCluster)) {
            LOG.error("HDFS is not enabled in either {} or {} cluster", localCluster.getName(),
                    remoteCluster.getName());
        }

        boolean isLocalHAEnbaled = ClusterHelper.isHDFSEnabled(localCluster) && ClusterHelper.isHighlyAvailableHDFS(
                localCustomProps);
        boolean isRemoteHAEnabled = ClusterHelper.isHDFSEnabled(remoteCluster)
                && ClusterHelper.isHighlyAvailableHDFS(remoteCustomProps);
        if (isLocalHAEnbaled ^ isRemoteHAEnabled) {
            LOG.warn("NameNode HA is not enabled in either {} or {} cluster", localCluster.getName(),
                    remoteCluster.getName());
        } else if (isLocalHAEnbaled) {
            validateNameserviceConfig(localCustomProps, remoteCustomProps);
        }
        BeaconCluster beaconLocalCluster = new BeaconCluster(localCluster);
        BeaconCluster beaconRemoteCluster = new BeaconCluster(remoteCluster);
        if (ClusterHelper.isHiveEnabled(beaconLocalCluster)
                && !(Boolean.valueOf(remoteCluster.getCustomProperties().getProperty(
                Cluster.ClusterFields.CLOUDDATALAKE.getName())))
                && (ClusterHelper.isHighlyAvailableHive(localCluster.getHsEndpoint())
                != ClusterHelper.isHighlyAvailableHive(remoteCluster.getHsEndpoint()))) {
            LOG.warn("Hive HA is not enabled in either {} or {} cluster", localCluster.getName(),
                    remoteCluster.getName());
        }
        if (ClusterHelper.isRangerEnabled(localCluster.getRangerEndpoint())
                != ClusterHelper.isRangerEnabled(remoteCluster.getRangerEndpoint())) {
            LOG.warn("Ranger is not enabled in either {} or {} cluster", localCluster.getName(),
                    remoteCluster.getName());
        }
        if (ClusterHelper.isHiveEnabled(beaconLocalCluster) != ClusterHelper.isHiveEnabled(beaconRemoteCluster)) {
            LOG.warn("Hive is not enabled in either {} or {} cluster", localCluster.getName(),
                    remoteCluster.getName());
        }
        if (ClusterHelper.isKerberized(localCluster) != ClusterHelper.isKerberized(remoteCluster)) {
            LOG.error("Kerberos is not enabled in either {} or {} cluster", localCluster.getName(),
                    remoteCluster.getName());
            throw new ValidationException("Kerberos is not enabled in either {} or {} cluster", localCluster.getName(),
                    remoteCluster.getName());
        }

        String localClusterKnoxProxyURL = ClusterHelper.getKnoxProxyURL(localCluster);
        String remoteClusterKnoxProxyURL = ClusterHelper.getKnoxProxyURL(remoteCluster);

        if ((StringUtils.isBlank(localClusterKnoxProxyURL) && StringUtils.isNotBlank(remoteCluster.getName()))
                || (StringUtils.isNotBlank(localClusterKnoxProxyURL)
                && StringUtils.isBlank(remoteClusterKnoxProxyURL))) {
            LOG.error("Knox proxy is not enabled in either {} or {} cluster", localCluster.getName(),
                    remoteCluster.getName());
        }
    }

    private static void validateNameserviceConfig(Properties localCustomProps, Properties remoteCustomProps)
            throws ValidationException{
        // Check that the nameservices on both the clusters aren't the same.
        List<String> localClusterNSs = ClusterHelper.getHDFSNameservicesList(localCustomProps);
        List<String> remoteClusterNSs = ClusterHelper.getHDFSNameservicesList(remoteCustomProps);
        Collection commonNSEntries = CollectionUtils.intersection(localClusterNSs, remoteClusterNSs);
        if (!commonNSEntries.isEmpty()) {
            throw new ValidationException("Local cluster nameservices {} and remote cluster nameservices {} "
                    + "can't have common nameservice.", localClusterNSs, remoteClusterNSs);
        }
    }

    public static void validationOnSubmission(ReplicationPolicy replicationPolicy,
                                              boolean validateCloud) throws BeaconException {
        validateIfAPIRequestAllowed(replicationPolicy);
        validateEntityDataset(replicationPolicy);
        validatePolicy(replicationPolicy, validateCloud);
    }

    public static void validatePolicyOnUpdate(ReplicationPolicy replicationPolicy,
                                              PropertiesIgnoreCase properties) throws BeaconException {
        isRequestAllowed(replicationPolicy);
        ValidationUtil.validatePolicyPropsUpdateAllowed(properties);
        validatePolicyFields(replicationPolicy, properties);
    }

    public static  void validatePolicyPropsUpdateAllowed(Properties properties)
            throws ValidationException {
        Set<String> allowedUpdateProps = ReplicationPolicyProperties.propsAllowedForUpdate();
        List propsNotAllowed = new LinkedList();
        for (Object prop : properties.keySet()) {
            if (!allowedUpdateProps.contains(prop)) {
                propsNotAllowed.add(prop);
            }
        }
        if (!propsNotAllowed.isEmpty()) {
            throw new ValidationException("Properties {} are not allowed to be updated.", propsNotAllowed);
        }
    }

    public static void validatePolicyFields(ReplicationPolicy existingPolicy, PropertiesIgnoreCase properties)
            throws BeaconException {
        String policyStatus = existingPolicy.getStatus();
        if (!(JobStatus.RUNNING.toString().equalsIgnoreCase(policyStatus)
                || JobStatus.SUBMITTED.toString().equalsIgnoreCase(policyStatus)
                || JobStatus.SUSPENDED.toString().equalsIgnoreCase(policyStatus))) {
            throw new BeaconException("Policy with a status [{}] can not be updated.", policyStatus);
        }
        String startTime = properties.getPropertyIgnoreCase(ReplicationPolicyProperties.STARTTIME.getName());
        Date oldStart = existingPolicy.getStartTime();
        Date newStart = null;
        if (StringUtils.isNotBlank(startTime)) {
            if (oldStart.before(new Date())) {
                throw new BeaconException("Start time can not be modified as the policy has already started");
            }
            newStart = DateUtil.parseDate(startTime);
            if (newStart.before(new Date())) {
                throw new BeaconException("Start time can not be earlier than current time");
            }
        } else {
            newStart = oldStart;
        }
        String endTimeStr = properties.getPropertyIgnoreCase(ReplicationPolicyProperties.ENDTIME.getName());
        Date newEnd = null;
        if (StringUtils.isNotBlank(endTimeStr)) {
            newEnd = DateUtil.parseDate(endTimeStr);
            if (newEnd.before(new Date())) {
                throw new BeaconException("End time cannot be earlier than current time");
            }
            if (newEnd.before(newStart)) {
                throw new IllegalArgumentException("End time can not be earlier than start time.");
            }
        } else {
            newEnd = existingPolicy.getEndTime();
        }
        //When startTime is being set but endTime is not.
        if (newEnd == existingPolicy.getEndTime() && newEnd.before(newStart)) {
            throw new IllegalArgumentException("Start time can not be later than end time.");
        }

        String frequencyInSecStr =  properties.getPropertyIgnoreCase(ReplicationPolicyProperties.FREQUENCY.getName());
        if (StringUtils.isNotBlank(frequencyInSecStr)) {
            Integer frequencyInSec = Integer.parseInt(frequencyInSecStr);
            int deftReplicationFrequencyInSec = BeaconConfig.getInstance().getScheduler().getMinReplicationFrequency();
            if (frequencyInSec < deftReplicationFrequencyInSec) {
                throw new BeaconException("Specified replication frequency {} should not be less than {} seconds",
                        frequencyInSec, deftReplicationFrequencyInSec);
            }
        }
        boolean enableSnapshotBasedRepl = Boolean.parseBoolean(
                properties.getPropertyIgnoreCase(FSDRProperties.ENABLE_SNAPSHOTBASED_REPLICATION.getName()));
        if (enableSnapshotBasedRepl) {
            DataSet dataSet = null;
            try {
                try {
                    dataSet = DataSet.create(existingPolicy.getSourceDataset(), existingPolicy.getSourceCluster(),
                            existingPolicy);
                    if (!PolicyHelper.isDatasetHCFS(existingPolicy.getSourceDataset())) {
                        String clusterName = existingPolicy.getSourceCluster();
                        Cluster cluster = clusterDao.getActiveCluster(clusterName);
                        boolean tdeEnabled = isTDEEnabled(cluster, existingPolicy.getSourceDataset(), existingPolicy);
                        if (tdeEnabled) {
                            throw new ValidationException(
                                    "Can not mark the source dataset snapshottable as it is TDE enabled");
                        }
                        FSSnapshotUtils.allowSnapshot(cluster, dataSet);
                    }
                } finally {
                    if (dataSet != null) {
                        dataSet.close();
                    }
                }
                if (!PolicyHelper.isDatasetHCFS(existingPolicy.getTargetDataset())) {
                    Cluster targetCluster = clusterDao.getActiveCluster(existingPolicy.getTargetCluster());
                    String targetDataset = existingPolicy.getTargetDataset();
                    try {
                        dataSet = DataSet.create(targetDataset, existingPolicy.getTargetCluster(), existingPolicy);
                        boolean isTargetEncrypted = dataSet.isEncrypted();
                        boolean targetSnapshottable = dataSet.isSnapshottable();
                        if (!isTargetEncrypted && !targetSnapshottable) {
                            FSSnapshotUtils.allowSnapshot(targetCluster, dataSet);
                        }
                    } finally {
                        if (dataSet != null) {
                            dataSet.close();
                        }
                    }
                }
            } catch (IOException ex) {
                throw new BeaconException("Unable to mark dataset as snapshottable:", ex.getMessage());
            }
        }
    }

    public static void validateWriteToPolicyCloudPath(ReplicationPolicy replicationPolicy,
                                                      String pathStr, String cluster)
            throws BeaconException {
        CloudCred cloudCred = getCloudCred(replicationPolicy);
        DataSet cloudDataSet = null;
        try {
            cloudDataSet = DataSet.create(pathStr, cluster, replicationPolicy);
            switch (cloudCred.getProvider()) {
                case AWS:
                case WASB:
                    cloudDataSet.isWriteAllowed();
                    break;
                default:
                    throw new ValidationException("Not a supported provider {}, for a file write access check",
                            cloudCred.getProvider());
            }
        } catch (BeaconException e) {
            throw new ValidationException(e, e.getMessage());
        } finally {
            if (cloudDataSet != null) {
                cloudDataSet.close();
            }
        }
    }

    private static CloudCred getCloudCred(ReplicationPolicy replicationPolicy) {
        String cloudCredId = replicationPolicy.getCloudCred();
        if (cloudCredId == null) {
            throw new IllegalArgumentException("Cloud cred id is missing.");
        }

        return cloudCredDao.getCloudCred(cloudCredId);
    }

    public static void validateIfAPIRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        if (policy == null) {
            throw new BeaconException("ReplicationPolicy cannot be null or empty");
        }

        isRequestAllowed(policy);
    }

    private static void isRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        if (policy.getType().equalsIgnoreCase(ReplicationType.FS.getName())) {
            isFSRequestAllowed(policy);
        } else if (policy.getType().equalsIgnoreCase(ReplicationType.HIVE.getName())) {
            isHiveRequestAllowed(policy);
        }
    }

    public static void isFSRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = ClusterHelper.getLocalCluster().getName();

        if (localClusterName.equalsIgnoreCase(sourceClusterName)
                && !PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            throw new BeaconException(
                    "This operation is not allowed on source cluster: {}. Try it on target cluster: {}",
                    sourceClusterName, targetClusterName);
        }
    }

    private static void isHiveRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = ClusterHelper.getLocalCluster().getName();

        if (!isDataLake(policy) && localClusterName.equalsIgnoreCase(sourceClusterName)) {
            throw new BeaconException(
                    "This operation is not allowed on source cluster: {}. Try it on target cluster: {}",
                    sourceClusterName, targetClusterName);
        }

        if (isDataLake(policy) && localClusterName.equalsIgnoreCase(targetClusterName)) {
            throw new BeaconException(
                    "This operation is not allowed on target cluster: {}. Try it on source cluster: {}",
                    targetClusterName, sourceClusterName);
        }
    }

    private static boolean isDataLake(ReplicationPolicy policy) throws BeaconException {
        if (policy.getType().equalsIgnoreCase(ReplicationType.HIVE.getName())) {
            Cluster tgtCluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
            return Boolean.valueOf(tgtCluster.getCustomProperties()
                    .getProperty(Cluster.ClusterFields.CLOUDDATALAKE.getName()));
        }
        return false;
    }

    private static void validatePolicy(final ReplicationPolicy policy, boolean validateCloud)
            throws BeaconException {
        updateListingCache(policy);
        ReplicationType replType = ReplicationHelper.getReplicationType(policy.getType());
        switch (replType) {
            case FS:
                FSPolicyHelper.validateFSReplicationProperties(FSPolicyHelper.buildFSReplicationProperties(policy));
                validateFSSourceDS(policy);
                validateFSTargetDS(policy);
                String cloudPath = null;
                String cluster = null;
                if (PolicyHelper.isDatasetHCFS(policy.getSourceDataset())) {
                    cloudPath = policy.getSourceDataset();
                    cluster = policy.getSourceCluster();
                } else if (PolicyHelper.isDatasetHCFS(policy.getTargetDataset())) {
                    cloudPath = policy.getTargetDataset();
                    cluster = policy.getTargetCluster();
                }
                if (validateCloud && cloudPath != null) {
                    DataSet cloudDataSet = null;
                    boolean cloudPathExists;
                    try {
                        cloudDataSet = DataSet.create(cloudPath, cluster, policy);
                        cloudPathExists = cloudDataSet.exists();
                    } catch (IOException e) {
                        throw new BeaconException(StringFormat.format("CloudPath {} doesn't exist", cloudPath));
                    } finally {
                        if (cloudDataSet != null) {
                            cloudDataSet.close();
                        }
                    }
                    DataSet sourceDataSet = null;
                    try {
                        sourceDataSet = DataSet.create(policy.getSourceDataset(),
                                policy.getSourceCluster(), policy);
                        if (!cloudPathExists
                                && (sourceDataSet instanceof S3FSDataSet || sourceDataSet instanceof WASBFSDataSet)) {
                            throw BeaconWebException.newAPIException(Response.Status.NOT_FOUND,
                                    "sourceDataset does not exist");
                        }
                    } finally {
                        if (sourceDataSet != null) {
                            sourceDataSet.close();
                        }
                    }
                    validateEncryptionAlgorithmType(policy);
                    if (FSUtils.isHCFS(new Path(policy.getTargetDataset()))) {
                        validateWriteToPolicyCloudPath(policy, cloudPath, policy.getTargetCluster());
                    }

                }
                if (cloudPath != null) {
                    ensureCloudEncryptionAndClusterTDECompatibility(policy);
                }
                break;
            case HIVE:
                HivePolicyHelper.validateHiveReplicationProperties(
                        HivePolicyHelper.buildHiveReplicationProperties(policy));
                validateDBSourceDS(policy);
                validateDBTargetDS(policy, validateCloud);
                break;
            default:
                throw new IllegalArgumentException("Invalid policy type: " + policy.getType());
        }
    }

    private static void ensureCloudEncryptionAndClusterTDECompatibility(ReplicationPolicy policy)
            throws BeaconException {
        Cluster cluster;
        String clusterDataSet;
        String cloudDataSet;
        String cloudCluster;
        if (StringUtils.isNotBlank(policy.getSourceCluster())){
            cluster  = ClusterHelper.getActiveCluster(policy.getSourceCluster());
            clusterDataSet = policy.getSourceDataset();
            cloudDataSet = policy.getTargetDataset();
            cloudCluster = policy.getTargetCluster();
        } else {
            cluster  = ClusterHelper.getActiveCluster(policy.getTargetCluster());
            clusterDataSet = policy.getTargetDataset();
            cloudDataSet = policy.getSourceDataset();
            cloudCluster = policy.getSourceCluster();
        }
        boolean tdeOn = isTDEEnabled(cluster, clusterDataSet, policy);
        DataSet dataSet = null;
        try {
            dataSet = DataSet.create(cloudDataSet, cloudCluster, policy);
            boolean encOn = dataSet.isEncrypted();
            if ((tdeOn ^ encOn) && !(dataSet instanceof WASBFSDataSet)) {
                if (tdeOn && clusterDataSet.equals(policy.getSourceDataset())) {
                    throw new BeaconException("Source data set is TDE enabled but target is not encryption enabled");
                }
                if (encOn && clusterDataSet.equals(policy.getTargetDataset())) {
                    throw new BeaconException("Source data set is encrypted but target cluster is not TDE enabled");
                }
            }
        } finally {
            if (dataSet != null) {
                dataSet.close();
            }

        }
    }

    private static void ensureClusterTDECompatibilityForHive(ReplicationPolicy policy)
            throws BeaconException {
        Cluster targetCluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
        DataSet dataSet = null;
        boolean tdeOn;
        try {
            dataSet = DataSet.create(policy.getSourceDataset(),
                    policy.getSourceCluster(), policy);
            tdeOn = dataSet.isEncrypted();
        } finally {
            if (dataSet != null) {
                dataSet.close();
            }
        }
        boolean encOn = ClusterHelper.isCloudEncryptionEnabled(policy.getTargetDataset(), targetCluster, policy);
        if (!encOn) {
            encOn = !EncryptionAlgorithmType.NONE.equals(PolicyHelper.getCloudEncryptionAlgorithm(policy));
        }
        if (tdeOn ^ encOn) {
            if (tdeOn) {
                throw new BeaconException("Source data set is TDE enabled but target is not encryption enabled");
            }
        }
    }


    private static void validateEncryptionAlgorithmType(ReplicationPolicy policy)
            throws BeaconException {
        Properties cloudEncProps = new Properties();

        if (policy.getCloudEncryptionAlgorithm() != null) {
            cloudEncProps.put(CLOUD_ENCRYPTIONALGORITHM.getName(), policy.getCloudEncryptionAlgorithm());
        }

        if (policy.getCloudEncryptionKey() != null) {
            cloudEncProps.put(CLOUD_ENCRYPTIONKEY.getName(), policy.getCloudEncryptionKey());
        }
        // When a sourceDataset is on Cloud, beacon doesn't need an encryption key and hence that is not mandatory.
        boolean isKeyMandatory = true;
        if (PolicyHelper.isDatasetHCFS(policy.getSourceDataset())) {
            isKeyMandatory = false;
        }
        validateEncryptionAlgorithmType(cloudEncProps, isKeyMandatory);
    }

    public static void validateEncryptionAlgorithmType(Cluster cluster) throws ValidationException {
        Properties cloudEncProps = new Properties();
        BeaconCluster beaconCluster = new BeaconCluster(cluster);
        if (beaconCluster.getHiveCloudEncryptionAlgorithm() != null) {
            cloudEncProps.put(CLOUD_ENCRYPTIONALGORITHM.getName(), beaconCluster.getHiveCloudEncryptionAlgorithm());
        }
        if (beaconCluster.getHiveCloudEncryptionKey() != null) {
            cloudEncProps.put(CLOUD_ENCRYPTIONKEY.getName(), beaconCluster.getHiveCloudEncryptionKey());
        }
        ValidationUtil.validateEncryptionAlgorithmType(cloudEncProps, true);
    }

    private static void validateEncryptionAlgorithmType(Properties cloudEncProps, boolean isKeyMandatory)
            throws ValidationException {
        String encryptionAlgorithm = cloudEncProps.getProperty(CLOUD_ENCRYPTIONALGORITHM.getName());
        String encryptionKey = cloudEncProps.getProperty(CLOUD_ENCRYPTIONKEY.getName());
        if (StringUtils.isEmpty(encryptionAlgorithm)) {
            if (StringUtils.isNotEmpty(encryptionKey)) {
                throw new ValidationException(
                        "Cloud Encryption key without a cloud encryption algorithm is not allowed");
            }
            return;
        }
        try {
            EncryptionAlgorithmType encryptionAlgorithmType = EncryptionAlgorithmType.valueOf(encryptionAlgorithm);
            switch (encryptionAlgorithmType) {
                case AWS_SSEKMS:
                    if (StringUtils.isEmpty(encryptionKey) && isKeyMandatory) {
                        throw new ValidationException(
                                "Cloud Encryption key is mandatory with this cloud encryption algorithm");
                    }
                    break;
                default:
                    if (StringUtils.isNotEmpty(encryptionKey)) {
                        throw new ValidationException(
                                "Cloud encryption key is not applicable to this cloud encryption algorithm",
                                encryptionAlgorithm);
                    }
                    break;
            }
        } catch (IllegalArgumentException iaEx) {
            throw new ValidationException("Cloud encryption algorithm {} is not supported", encryptionAlgorithm, iaEx);
        }
    }

    private static void validateFSSourceDS(ReplicationPolicy policy) throws BeaconException {
        String sourceDataset = policy.getSourceDataset();
        DataSet srcDataSet = null;
        try {
            srcDataSet = DataSet.create(sourceDataset, policy.getSourceCluster(), policy);
            if (!srcDataSet.exists()) {
                throw new ValidationException("Source dataset {} doesn't exists.", policy.getSourceDataset());
            }
            if (srcDataSet.isSnapshottable()) {
                LOG.info("Deleting existing snapshot(s) on source directory.");
                try {
                    String snapshotNamePrefix = FSSnapshotUtils.getSnapshotNamePrefix(policy.getName());
                    srcDataSet.deleteAllSnapshots(snapshotNamePrefix);
                } catch (IOException e) {
                    throw new BeaconException("Error while deleting existing snapshot(s).", e);
                }
            }
            if (!PolicyHelper.isDatasetHCFS(sourceDataset)) {
                String clusterName = policy.getSourceCluster();
                Cluster cluster = clusterDao.getActiveCluster(clusterName);
                boolean tdeEnabled = isTDEEnabled(cluster, sourceDataset, policy);
                boolean markSourceSnapshottable = Boolean.valueOf(policy.getCustomProperties().getProperty(
                        FSDRProperties.ENABLE_SNAPSHOTBASED_REPLICATION.getName()));
                if (tdeEnabled && markSourceSnapshottable) {
                    throw new ValidationException("Can not mark the source dataset snapshottable as it is TDE enabled");
                }
                if (tdeEnabled) {
                    policy.getCustomProperties().setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
                }
                if (markSourceSnapshottable) {
                    FSSnapshotUtils.allowSnapshot(cluster, srcDataSet);
                }
            }
        } catch (IOException e) {
            throw new  ValidationException(e, "Dataset {} doesn't exists.", sourceDataset);
        } finally {
            if (srcDataSet != null) {
                srcDataSet.close();
            }
        }
    }

    private static boolean isTDEEnabled(Cluster cluster, String dataset, ReplicationPolicy policy)
            throws BeaconException {
        DataSet dataSet = null;
        try {
            dataSet = DataSet.create(dataset, cluster.getName(), policy);
            return dataSet.isEncrypted();
        } finally {
            if (dataSet != null) {
                dataSet.close();
            }
        }
    }

    private static void validateEntityDataset(final ReplicationPolicy policy) throws BeaconException {
        BeaconNotification notification = new BeaconNotification();
        // TODO : Handle cases when multiple cloud object store are in picture.
        boolean targetDatasetConflicted = ReplicationUtils.isDatasetConflicting(ReplicationHelper.getReplicationType(
                policy.getType()), policy.getTargetDataset(), Destination.TARGET);
        if (targetDatasetConflicted) {
            notification.addError("Target dataset already in replication, " + policy.getTargetDataset());
        }

        boolean sourceDataConflictAndTgtClusterConflict = ReplicationUtils
                .isSourceDataConflictAndTgtClusterConflict(policy);
        if (sourceDataConflictAndTgtClusterConflict) {
            notification.addError(StringFormat.format("Source dataset {} already in replication"
                            + " on same target cluster {}",
                    policy.getSourceDataset(), policy.getTargetCluster()));
        }
        // TODO : Check if a target dataset is source for another policy and vice versa.
        if (notification.hasErrors()) {
            throw new BeaconException(notification.errorMessage());
        }
    }

    private static String getDataset(ReplicationPolicy policy, Destination dest) {
        return dest == Destination.SOURCE ? policy.getSourceDataset() : policy.getTargetDataset();
    }

    public static FileSystem getFileSystem(String clusterName) throws BeaconException {
        Cluster cluster = clusterDao.getActiveCluster(clusterName);
        return FSUtils.getFileSystem(cluster.getFsEndpoint(), new Configuration());
    }


    private static void validateFSTargetDS(ReplicationPolicy policy) throws BeaconException {
        boolean isHCFS = PolicyHelper.isPolicyHCFS(policy);
        String targetDataset = policy.getTargetDataset();
        DataSet tgtDataSet = null;
        try {
            tgtDataSet = DataSet.create(targetDataset, policy.getTargetCluster(), policy);
            boolean targetPathExists = false;
            if (tgtDataSet.exists()) {
                if (!tgtDataSet.isEmpty()) {
                    throw new ValidationException("Target dataset directory {} is not empty.", targetDataset);
                }
                targetPathExists = true;
            }
            if (isHCFS) {
                if (!targetPathExists) {
                    // Default permission would be set to (777 & !022) = 755.
                    tgtDataSet.create();
                }
                return;
            }
            String clusterName = policy.getTargetCluster();
            Cluster sourceCluster = clusterDao.getActiveCluster(policy.getSourceCluster());
            Cluster targetCluster = clusterDao.getActiveCluster(clusterName);
            String sourceDataset = policy.getSourceDataset();
            boolean isSourceEncrypted = Boolean.valueOf(policy.getCustomProperties().getProperty(
                    FSDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
            boolean isTargetEncrypted = isTDEEnabled(targetCluster, targetDataset, policy);
            if (isTargetEncrypted && !isSourceEncrypted) {
                policy.getCustomProperties()
                        .setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
            }
            boolean sourceSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(sourceCluster.getName(),
                    FSUtils.getStagingUri(sourceCluster.getFsEndpoint(), sourceDataset));
            LOG.info("Is source directory: {} snapshottable: {}", sourceDataset, sourceSnapshottable);
            if (isSourceEncrypted && !isTargetEncrypted) {
                throw new ValidationException("Target dataset directory {} is not encrypted.", targetDataset);
            }
            if (!targetPathExists) {
                createTargetFSDirectory(policy);
            }
            boolean targetSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(clusterName, FSUtils
                    .getStagingUri(targetCluster.getFsEndpoint(), targetDataset));
            if (!isTargetEncrypted && sourceSnapshottable && !targetSnapshottable) {
                FSSnapshotUtils.allowSnapshot(targetCluster, tgtDataSet);
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        } finally {
            if (tgtDataSet != null) {
                tgtDataSet.close();
            }
        }
    }

    private static void validateDBTargetDS(ReplicationPolicy policy, boolean validateCloud) throws BeaconException {
        Cluster cluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
        String targetDataset = policy.getTargetDataset();

        HiveMetadataClient hiveClient = null;
        try {
            hiveClient = HiveClientFactory.getMetadataClient(cluster);
            boolean dbExists = hiveClient.doesDBExist(targetDataset);
            if (dbExists) {
                List<String> tables = hiveClient.getTables(targetDataset);
                if (!tables.isEmpty()) {
                    throw new ValidationException("Target Hive server already has dataset {} with tables",
                            targetDataset);
                }
            } else {
                validateHiveTargetDataSetName(targetDataset);
            }
            BeaconCluster beaconCluster = new BeaconCluster(cluster);
            boolean isHCFS = PolicyHelper.isDatasetHCFS(beaconCluster.getHiveWarehouseLocation());
            if (isHCFS) {
                if (validateCloud) {
                    validateEncryptionAlgorithmType(policy);
                    validateWriteToPolicyCloudPath(policy, beaconCluster.getHiveWarehouseLocation(),
                            policy.getTargetCluster());
                }
                ensureClusterTDECompatibilityForHive(policy);
            } else {
                boolean sourceEncrypted = Boolean.valueOf(policy.getCustomProperties().getProperty(FSDRProperties
                        .TDE_ENCRYPTION_ENABLED.getName()));
                if (dbExists && sourceEncrypted) {
                    DataSet dataSet = null;
                    try {
                        dataSet = DataSet.create(targetDataset, cluster.getName(), policy);
                        if (!dataSet.isEncrypted()) {
                            throw new ValidationException("Target dataset DB {} is not encrypted.",
                                    targetDataset);
                        }
                    } finally {
                        if (dataSet != null) {
                            dataSet.close();
                        }
                    }
                }
            }
        } finally {
            HiveClientFactory.close(hiveClient);
        }
    }

    private static void validateHiveTargetDataSetName(String hiveDBName) throws ValidationException {
        String alphaNumUnderscoreRegEx = "^[a-zA-Z0-9_]*$";
        if (hiveDBName != null) {
            String hiveDBwithoutEscaping = hiveDBName;
            if (hiveDBwithoutEscaping.startsWith("`") && hiveDBwithoutEscaping.endsWith("`")) {
                hiveDBwithoutEscaping = hiveDBName.substring(1, hiveDBName.length()-1);
            }
            if (hiveDBwithoutEscaping.matches(alphaNumUnderscoreRegEx)) {
                return;
            }
        }
        throw new ValidationException("Hive target dataset name {} is invalid:", hiveDBName);
    }
    private static void validateDBSourceDS(ReplicationPolicy policy) throws BeaconException {
        String sourceDataset = policy.getSourceDataset();
        DataSet dataSet = null;
        try {
            dataSet = DataSet.create(sourceDataset, policy.getSourceCluster(), policy);
            if (dataSet.isEncrypted()) {
                policy.getCustomProperties().setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
            }
        } finally {
            if (dataSet != null) {
                dataSet.close();
            }
        }
    }


    private static void createTargetFSDirectory(ReplicationPolicy policy) throws BeaconException {
        LOG.info("Creating a data directory on target file system: {}", policy.getTargetDataset());
        Cluster sourceCluster = clusterDao.getActiveCluster(policy.getSourceCluster());
        Cluster targetCluster = clusterDao.getActiveCluster(policy.getTargetCluster());
        String sourceDataset = FSUtils.getStagingUri(sourceCluster.getFsEndpoint(), policy.getSourceDataset());
        String targetDataSet = FSUtils.getStagingUri(targetCluster.getFsEndpoint(), policy.getTargetDataset());
        try {
            FileSystem sourceFS = FSUtils.getFileSystem(sourceCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(sourceCluster));
            FileSystem targetFS = FSUtils.getFileSystem(targetCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(targetCluster));
            FileStatus fsStatus = sourceFS.getFileStatus(new Path(sourceDataset));
            Configuration conf = ClusterHelper.getHAConfigurationOrDefault(targetCluster);
            conf.set(BeaconConstants.FS_DEFAULT_NAME_KEY, targetCluster.getFsEndpoint());
            FSSnapshotUtils.createFSDirectory(targetFS, fsStatus.getPermission(),
                    fsStatus.getOwner(), fsStatus.getGroup(), targetDataSet);
        } catch (IOException e) {
            LOG.error("Exception occurred while creating a directory on target: {}", e);
            throw new BeaconException("Exception occurred while creating a directory on target: ", e);
        }
    }

    private static void updateListingCache(ReplicationPolicy policy) throws BeaconException {
        updateListingCache(policy.getSourceCluster());
        updateListingCache(policy.getTargetCluster());
    }

    private static void updateListingCache(String clusterName) throws BeaconException {
        if (StringUtils.isNotEmpty(clusterName)) {
            Cluster cluster = clusterDao.getActiveCluster(clusterName);
            if (StringUtils.isNotEmpty(cluster.getFsEndpoint())) {
                SnapshotListing.get().updateListing(clusterName, cluster.getFsEndpoint(), Path.SEPARATOR);
                EncryptionZoneListing.get().updateListing(clusterName, cluster.getFsEndpoint(),
                        Path.SEPARATOR);
            }
        }
    }
}
