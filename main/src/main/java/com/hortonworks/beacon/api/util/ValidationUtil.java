/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */


package com.hortonworks.beacon.api.util;

import com.hortonworks.beacon.api.EncryptionZoneListing;
import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.hive.HiveMetadataClient;
import com.hortonworks.beacon.hive.HiveMetadataClientFactory;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.replication.fs.FSPolicyHelper;
import com.hortonworks.beacon.replication.fs.FSSnapshotUtils;
import com.hortonworks.beacon.replication.fs.SnapshotListing;
import com.hortonworks.beacon.replication.hive.HivePolicyHelper;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

/**
 * Utility class to validate API requests.
 */
public final class ValidationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationUtil.class);
    private static ClusterDao clusterDao = new ClusterDao();


    private ValidationUtil() {
    }

    public static void validateClusterPairing(Cluster localCluster, Cluster remoteCluster) {
        Properties localCustomProperties = localCluster.getCustomProperties();
        Properties remoteCustomProperties = remoteCluster.getCustomProperties();
        if (ClusterHelper.isHighlyAvailableHDFS(localCustomProperties)
                != ClusterHelper.isHighlyAvailableHDFS(remoteCustomProperties)) {
            LOG.error("NameNode HA is not enabled in either {} or {} cluster", localCluster.getName(),
                remoteCluster.getName());
        }
        if (ClusterHelper.isHiveEnabled(localCluster.getHsEndpoint())
                && (ClusterHelper.isHighlyAvailableHive(localCluster.getHsEndpoint())
                != ClusterHelper.isHighlyAvailableHive(remoteCluster.getHsEndpoint()))) {
            LOG.error("Hive HA is not enabled in either {} or {} cluster", localCluster.getName(),
                remoteCluster.getName());
        }
        if (ClusterHelper.isRangerEnabled(localCluster.getRangerEndpoint())
                != ClusterHelper.isRangerEnabled(remoteCluster.getRangerEndpoint())) {
            LOG.error("Ranger is not enabled in either {} or {} cluster", localCluster.getName(),
                remoteCluster.getName());
        }
        if (StringUtils.isBlank(localCluster.getHsEndpoint())
                != StringUtils.isBlank(remoteCluster.getHsEndpoint())) {
            LOG.error("Hive is not enabled in either {} or {} cluster", localCluster.getName(),
                remoteCluster.getName());
        }
        if (ClusterHelper.isKerberized(localCluster) != ClusterHelper.isKerberized(remoteCluster)) {
            LOG.error("Kerberos is not enabled in either {} or {} cluster", localCluster.getName(),
                remoteCluster.getName());
        }
    }

    public static void validationOnSubmission(ReplicationPolicy replicationPolicy) throws BeaconException {
        validateIfAPIRequestAllowed(replicationPolicy);
        validatePolicy(replicationPolicy);
        validateEntityDataset(replicationPolicy);
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

    private static void isFSRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = ClusterHelper.getLocalCluster().getName();

        if (localClusterName.equalsIgnoreCase(sourceClusterName)
                && !PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            throw BeaconWebException.newAPIException(
                    "This operation is not allowed on source cluster: {}. Try it on target cluster: {}",
                    sourceClusterName, targetClusterName);
        }
    }

    private static void isHiveRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = ClusterHelper.getLocalCluster().getName();

        if (!isDataLake(policy) && localClusterName.equalsIgnoreCase(sourceClusterName)) {
            throw BeaconWebException.newAPIException(
                    "This operation is not allowed on source cluster: {}. Try it on target cluster: {}",
                    sourceClusterName, targetClusterName);
        }

        if (isDataLake(policy) && localClusterName.equalsIgnoreCase(targetClusterName)) {
            throw BeaconWebException.newAPIException(
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

    private static void validatePolicy(final ReplicationPolicy policy) throws BeaconException {
        updateSnapshotCache(policy);
        ReplicationType replType = ReplicationHelper.getReplicationType(policy.getType());
        switch (replType) {
            case FS:
                FSPolicyHelper.validateFSReplicationProperties(FSPolicyHelper.buildFSReplicationProperties(policy));
                if (!FSUtils.isHCFS(new Path(policy.getSourceDataset()))) {
                    validateFSSourceDS(policy);
                }
                if (!FSUtils.isHCFS(new Path(policy.getTargetDataset()))) {
                    validateFSTargetDS(policy);
                }
                break;
            case HIVE:
                HivePolicyHelper.validateHiveReplicationProperties(
                        HivePolicyHelper.buildHiveReplicationProperties(policy));
                validateDBSourceDS(policy);
                validateDBTargetDS(policy);
                break;
            default:
                throw new IllegalArgumentException("Invalid policy type: " + policy.getType());
        }
    }

    private static void validateFSSourceDS(ReplicationPolicy policy) throws BeaconException {
        String clusterName = policy.getSourceCluster();
        String sourceDataset = policy.getSourceDataset();
        Cluster cluster = clusterDao.getActiveCluster(clusterName);
        try {
            FileSystem fileSystem = FSUtils.getFileSystem(cluster.getFsEndpoint(), ClusterHelper
                    .getHAConfigurationOrDefault(cluster), false);
            if (!fileSystem.exists(new Path(sourceDataset))) {
                throw new ValidationException("Dataset {} doesn't exists in {} cluster", policy.getSourceDataset(),
                        policy.getSourceCluster());
            }
            boolean tdeEnabled = isTDEEnabled(cluster, sourceDataset);
            boolean markSourceSnapshottable = Boolean.valueOf(policy.getCustomProperties().getProperty(FSDRProperties
                                     .SOURCE_SETSNAPSHOTTABLE.getName()));
            boolean snapshottable = FSSnapshotUtils.checkSnapshottableDirectory(clusterName, FSUtils.getStagingUri(
                    cluster.getFsEndpoint(), sourceDataset));
            if (tdeEnabled && (snapshottable || markSourceSnapshottable)) {
                throw new ValidationException("TDE enabled zone can't be used for snapshot based replication.");
            }
            if (tdeEnabled) {
                policy.getCustomProperties().setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
            }
            if (markSourceSnapshottable) {
                FSSnapshotUtils.allowSnapshot(ClusterHelper.getHAConfigurationOrDefault(clusterName), sourceDataset,
                        new URI(cluster.getFsEndpoint()), cluster);
            }
        } catch (IOException e) {
            throw new  ValidationException(e, "Dataset {} doesn't exists.", sourceDataset);
        } catch (URISyntaxException | InterruptedException e) {
            throw new BeaconException(e);
        }
    }

    private static boolean isTDEEnabled(Cluster cluster, String dataset) throws BeaconException {
        String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(cluster.getName(),
                cluster.getFsEndpoint(), dataset);
        return StringUtils.isNotEmpty(baseEncryptedPath);
    }

    private static void validateEntityDataset(final ReplicationPolicy policy) throws BeaconException {
        checkDatasetConfliction(policy);
    }

    private static void checkDatasetConfliction(ReplicationPolicy policy) throws BeaconException {
        boolean isConflicted = ReplicationUtils.isDatasetConflicting(
                ReplicationHelper.getReplicationType(policy.getType()), policy.getSourceDataset());
        if (isConflicted) {
            LOG.error("Dataset {} is already in replication", policy.getSourceDataset());
            throw new BeaconException("Dataset {} is already in replication", policy.getSourceDataset());
        }
    }

    private static void validateFSTargetDS(ReplicationPolicy policy) throws BeaconException {
        String clusterName = policy.getTargetCluster();
        String targetDataset = policy.getTargetDataset();
        Cluster targetCluster = clusterDao.getActiveCluster(clusterName);
        boolean isEncrypted;
        try {
            FileSystem fileSystem = FSUtils.getFileSystem(targetCluster.getFsEndpoint(), new Configuration(), false);
            if (fileSystem.exists(new Path(targetDataset))) {
                RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(targetDataset), true);
                if (files != null && files.hasNext()) {
                    throw new ValidationException("Target dataset directory {} is not empty.", targetDataset);
                }
                if (!FSUtils.isHCFS(new Path(policy.getSourceDataset()))) {
                    String sourceDataset = policy.getSourceDataset();
                    Cluster sourceCluster = clusterDao.getActiveCluster(policy.getSourceCluster());
                    isEncrypted = isTDEEnabled(targetCluster, targetDataset);
                    boolean sourceSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(sourceCluster.getName(),
                            FSUtils.getStagingUri(sourceCluster.getFsEndpoint(), sourceDataset));
                    boolean targetSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(clusterName, FSUtils
                            .getStagingUri(targetCluster.getFsEndpoint(), targetDataset));
                    if (isEncrypted && (sourceSnapshottable || targetSnapshottable)) {
                        throw new ValidationException("TDE enabled zone can't be used for snapshot based replication.");
                    }
                    if (sourceSnapshottable && !targetSnapshottable) {
                        FSSnapshotUtils.allowSnapshot(ClusterHelper.getHAConfigurationOrDefault(clusterName),
                                targetDataset, new URI(targetCluster.getFsEndpoint()), targetCluster);
                    }
                    if (Boolean.valueOf(policy.getCustomProperties().getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED
                            .getName())) && !isEncrypted) {
                        throw new ValidationException("Target dataset directory {} is not encrypted.", targetDataset);
                    } else if (isEncrypted) {
                        policy.getCustomProperties()
                                .setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
                    }
                }
            } else {
                createTargetFSDirectory(policy);
            }
        } catch (IOException | InterruptedException | URISyntaxException e) {
            throw new BeaconException(e);
        }
    }

    private static void validateDBTargetDS(ReplicationPolicy policy) throws BeaconException {
        Cluster cluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
        String targetDataset = policy.getTargetDataset();

        HiveMetadataClient hiveClient = null;
        try {
            hiveClient = HiveMetadataClientFactory.getClient(cluster);
            boolean dbExists = hiveClient.doesDBExist(targetDataset);

            List<String> tables = hiveClient.getTables(targetDataset);
            if (dbExists && !tables.isEmpty()) {
                throw new ValidationException("Target Hive server already has dataset {} with tables", targetDataset);
            }

            boolean sourceEncrypted = Boolean.valueOf(policy.getCustomProperties().getProperty(FSDRProperties
                    .TDE_ENCRYPTION_ENABLED.getName()));
            if (dbExists && sourceEncrypted) {
                Path dbLocation = hiveClient.getDatabaseLocation(targetDataset);
                String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(
                        cluster.getName(), cluster.getFsEndpoint(), dbLocation.toString());
                if (StringUtils.isEmpty(baseEncryptedPath)) {
                    throw new ValidationException("Target dataset DB {} is not encrypted.",
                            targetDataset);
                }
            }
        } finally {
            HiveMetadataClientFactory.close(hiveClient);
        }
    }

    private static void validateDBSourceDS(ReplicationPolicy policy) throws BeaconException {
        Cluster cluster = ClusterHelper.getActiveCluster(policy.getSourceCluster());
        String sourceDataset = policy.getSourceDataset();

        HiveMetadataClient hiveClient = null;
        try {
            hiveClient = HiveMetadataClientFactory.getClient(cluster);
            Path dbPath = hiveClient.getDatabaseLocation(sourceDataset);
            String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(cluster.getName(),
                    cluster.getFsEndpoint(), dbPath.toUri().getPath());
            if (StringUtils.isNotEmpty(baseEncryptedPath)) {
                policy.getCustomProperties().setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
            }
        } finally {
            HiveMetadataClientFactory.close(hiveClient);
        }
    }


    private static void createTargetFSDirectory(ReplicationPolicy policy) throws BeaconException, IOException {
        LOG.info("Creating snapshot data directory on target file system: {}", policy.getTargetDataset());
        String sourceDataset = policy.getSourceDataset();
        String targetDataSet = policy.getTargetDataset();
        if (FSUtils.isHCFS(new Path(sourceDataset))) {
            Cluster targetCluster = clusterDao.getActiveCluster(policy.getTargetCluster());
            FileSystem targetFS = FSUtils.getFileSystem(targetCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(targetCluster), false);
            UserGroupInformation user = UserGroupInformation.getLoginUser();
            FSSnapshotUtils.createFSDirectory(targetFS, FsPermission.getDirDefault(),
                    user.getShortUserName(), user.getShortUserName(), targetDataSet);
            return;
        }
        Cluster sourceCluster = clusterDao.getActiveCluster(policy.getSourceCluster());
        Cluster targetCluster = clusterDao.getActiveCluster(policy.getTargetCluster());
        sourceDataset = FSUtils.getStagingUri(sourceCluster.getFsEndpoint(), policy.getSourceDataset());
        targetDataSet = FSUtils.getStagingUri(targetCluster.getFsEndpoint(), policy.getTargetDataset());

        try {
            FileSystem sourceFS = FSUtils.getFileSystem(sourceCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(sourceCluster), false);
            FileSystem targetFS = FSUtils.getFileSystem(targetCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(targetCluster), false);

            boolean isSourceDirSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(sourceCluster.getName(),
                    sourceDataset);
            LOG.info("Is source directory: {} snapshottable: {}", sourceDataset, isSourceDirSnapshottable);

            FileStatus fsStatus = sourceFS.getFileStatus(new Path(sourceDataset));
            Configuration conf = ClusterHelper.getHAConfigurationOrDefault(targetCluster);
            conf.set(BeaconConstants.FS_DEFAULT_NAME_KEY, targetCluster.getFsEndpoint());
            FSSnapshotUtils.createFSDirectory(targetFS, fsStatus.getPermission(),
                    fsStatus.getOwner(), fsStatus.getGroup(), targetDataSet);
            if (isSourceDirSnapshottable) {
                FSSnapshotUtils.allowSnapshot(conf, targetDataSet, targetFS.getUri(), targetCluster);
            }
        } catch (IOException | InterruptedException e) {
            LOG.error("Exception occurred while creating snapshottable directory on target: {}", e);
            throw new BeaconException("Exception occurred while creating snapshottable directory on target: ", e);
        }
    }

    private static void updateSnapshotCache(ReplicationPolicy policy) throws BeaconException {
        if (!PolicyHelper.isDatasetHCFS(policy.getSourceDataset())) {
            Cluster sourceCluster = clusterDao.getActiveCluster(policy.getSourceCluster());
            SnapshotListing.get().updateListing(sourceCluster.getName(), sourceCluster.getFsEndpoint(), Path.SEPARATOR);
        }
        if (!PolicyHelper.isDatasetHCFS(policy.getTargetDataset())) {
            Cluster targetCluster = clusterDao.getActiveCluster(policy.getTargetCluster());
            SnapshotListing.get().updateListing(targetCluster.getName(), targetCluster.getFsEndpoint(), Path.SEPARATOR);
        }
    }
}
