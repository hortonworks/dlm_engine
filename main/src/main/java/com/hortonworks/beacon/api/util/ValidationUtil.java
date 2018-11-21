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

import com.hortonworks.beacon.api.PropertiesIgnoreCase;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.entity.ClusterValidator;
import com.hortonworks.beacon.entity.EncryptionAlgorithmType;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.ReplicationPolicyProperties;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.CloudCredDao;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationType;
import com.hortonworks.dlmengine.BeaconReplicationPolicy;
import com.hortonworks.dlmengine.DataSet;
import com.hortonworks.dlmengine.fs.HCFSDataset;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static void validatePolicyOnUpdate(BeaconReplicationPolicy replicationPolicy,
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

    public static void validatePolicyFields(BeaconReplicationPolicy existingPolicy, PropertiesIgnoreCase properties)
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
            DataSet sourceDatasetV2 = existingPolicy.getSourceDatasetV2();
            if (!(sourceDatasetV2 instanceof HCFSDataset)) {
                boolean tdeEnabled = sourceDatasetV2.isEncrypted();
                if (tdeEnabled) {
                    throw new ValidationException(
                            "Can not mark the source dataset snapshottable as it is TDE enabled");
                }
                sourceDatasetV2.allowSnapshot();
            }
            DataSet targetDatasetV2 = existingPolicy.getTargetDatasetV2();
            if (!(targetDatasetV2 instanceof HCFSDataset)) {
                boolean isTargetEncrypted = targetDatasetV2.isEncrypted();
                if (isTargetEncrypted) {
                    throw new ValidationException(
                            "Can not mark the source dataset snapshottable as it is TDE enabled");
                }
                boolean targetSnapshottable = targetDatasetV2.isSnapshottable();
                if (!isTargetEncrypted && !targetSnapshottable) {
                    targetDatasetV2.allowSnapshot();
                }
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

    public static FileSystem getFileSystem(String clusterName) throws BeaconException {
        Cluster cluster = clusterDao.getActiveCluster(clusterName);
        return FSUtils.getFileSystem(cluster.getFsEndpoint(), new Configuration());
    }
}
