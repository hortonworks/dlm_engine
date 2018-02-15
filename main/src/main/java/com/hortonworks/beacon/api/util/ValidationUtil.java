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

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.api.EncryptionZoneListing;
import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.replication.fs.FSPolicyHelper;
import com.hortonworks.beacon.replication.fs.FSSnapshotUtils;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Utility class to validate API requests.
 */
public final class ValidationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationUtil.class);
    private static final String SHOW_DATABASES = "SHOW DATABASES";
    private static final String DESC_DATABASE = "DESC DATABASE ";
    private static final String SHOW_TABLES = "SHOW TABLES";
    private static final int DB_NOT_EXIST_EC = 10072;
    private static final String DB_NOT_EXIST_STATE = "42000";

    private static final String USE = "USE ";
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
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = ClusterHelper.getLocalCluster().getName();

        // If policy is HCFS or target cluster is cloud data lake then requests are allowed on source cluster
        if (localClusterName.equalsIgnoreCase(sourceClusterName)
                && !PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())
                && !isDataLake(policy)) {
            throw BeaconWebException.newAPIException(
                "This operation is not allowed on source cluster: {}. Try it on target cluster: {}", sourceClusterName,
                targetClusterName);
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
        ReplicationType replType = ReplicationHelper.getReplicationType(policy.getType());
        switch (replType) {
            case FS:
                FSPolicyHelper.validateFSReplicationProperties(FSPolicyHelper.buildFSReplicationProperties(policy));
                validateFSSourceDS(policy);
                validateFSTargetDS(policy);
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
        boolean datasetHCFS = PolicyHelper.isDatasetHCFS(policy.getSourceDataset());
        if (datasetHCFS) {
            return;
        }
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
                        new URI(cluster.getFsEndpoint()));
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
        boolean datasetHCFS = PolicyHelper.isDatasetHCFS(policy.getTargetDataset());
        if (datasetHCFS) {
            return;
        }
        String clusterName = policy.getTargetCluster();
        String targetDataset = policy.getTargetDataset();
        Cluster cluster = clusterDao.getActiveCluster(clusterName);
        boolean isEncrypted;
        try {
            FileSystem fileSystem = FSUtils.getFileSystem(cluster.getFsEndpoint(), new Configuration(), false);
            if (fileSystem.exists(new Path(targetDataset))) {
                RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(targetDataset), true);
                if (files != null && files.hasNext()) {
                    throw new ValidationException("Target dataset directory {} is not empty.", targetDataset);
                }
                isEncrypted = isTDEEnabled(cluster, targetDataset);
                if (Boolean.valueOf(policy.getCustomProperties().getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED
                    .getName())) && !isEncrypted) {
                    throw new ValidationException("Target dataset directory {} is not encrypted.", targetDataset);
                } else if (isEncrypted) {
                    policy.getCustomProperties().setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
                }
            } else {
                createTargetFSDirectory(policy);
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    private static void validateDBTargetDS(ReplicationPolicy policy) throws BeaconException {
        Cluster cluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
        String targetDataset = policy.getTargetDataset();
        String hsEndPoint = cluster.getHsEndpoint();
        HiveDRUtils.initializeDriveClass();
        String connString = HiveDRUtils.getHS2ConnectionUrl(hsEndPoint);
        Connection connection = null;
        Statement statement = null;
        try {
            connection = HiveDRUtils.getConnection(connString);
            statement = connection.createStatement();
            if (isDBExists(statement, targetDataset)) {
                statement.execute(USE + targetDataset);
                try (ResultSet res = statement.executeQuery(SHOW_TABLES)) {
                    if (res.next()) {
                        String tableName = res.getString(1);
                        if (StringUtils.isNotBlank(tableName)) {
                            throw new ValidationException("Target Hive server already has dataset {} with tables",
                                targetDataset);
                        }
                        if (Boolean.valueOf(policy.getCustomProperties().getProperty(FSDRProperties
                                .TDE_ENCRYPTION_ENABLED.getName()))) {
                            String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(
                                    cluster.getName(), cluster.getFsEndpoint(), targetDataset);
                            if (StringUtils.isEmpty(baseEncryptedPath)) {
                                throw new ValidationException("Target dataset DB {} is not encrypted.",
                                        targetDataset);
                            }
                        }
                    }
                }
            }
        } catch (ValidationException e) {
            throw e;
        } catch (SQLException sqe) {
            LOG.error("Exception occurred while validating Hive end point: {}", sqe.getMessage());
            throw new ValidationException(sqe, "Exception occurred while validating Hive end point: ");
        } finally {
            HiveDRUtils.cleanup(statement, connection);
        }
    }

    private static void validateDBSourceDS(ReplicationPolicy policy) throws BeaconException {
        Cluster cluster = ClusterHelper.getActiveCluster(policy.getSourceCluster());
        String sourceDataset = policy.getSourceDataset();
        String hsEndPoint = cluster.getHsEndpoint();
        HiveDRUtils.initializeDriveClass();
        String connString = HiveDRUtils.getHS2ConnectionUrl(hsEndPoint);
        Connection connection = null;
        Statement statement = null;
        try {
            connection = HiveDRUtils.getConnection(connString);
            statement = connection.createStatement();
            String dbPath = getDatabasePath(statement, sourceDataset, cluster.getName());
            if (StringUtils.isNotEmpty(dbPath)) {
                String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(cluster.getName(),
                        cluster.getFsEndpoint(), new Path(dbPath).toUri().getPath());
                if (StringUtils.isNotEmpty(baseEncryptedPath)) {
                    policy.getCustomProperties().setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
                }
            }
        } catch (SQLException sqe) {
            LOG.error("Exception occurred while validating source Hive DB: {}", sqe.getMessage());
            throw new ValidationException(sqe, "Exception occurred while validating source Hive DB: ");
        } finally {
            HiveDRUtils.cleanup(statement, connection);
        }
    }


    private static boolean isDBExists(final Statement statement, String dataset) throws SQLException {
        try (ResultSet res = statement.executeQuery(SHOW_DATABASES)) {
            while (res.next()) {
                String dbName = res.getString(1);
                if (dbName.equals(dataset)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String getDatabasePath(final Statement statement, String dataset, String clusterName) throws
            SQLException, ValidationException {
        String query = DESC_DATABASE + dataset;
        String dbPath= null;
        try (ResultSet res = statement.executeQuery(query)) {
            if (res.next()) {
                dbPath = res.getString(3);
            }
        } catch (SQLException sqe) {
            if (sqe.getErrorCode() == DB_NOT_EXIST_EC && sqe.getSQLState().equalsIgnoreCase(DB_NOT_EXIST_STATE)) {
                throw new ValidationException(sqe, "Database {} doesn't exists on cluster {}", dataset, clusterName);
            } else {
                throw sqe;
            }
        }
        LOG.debug("Database: {}, path: {}", dataset, dbPath);
        return dbPath;
    }

    private static void createTargetFSDirectory(ReplicationPolicy policy) throws BeaconException, IOException {
        LOG.info("Creating snapshot data directory on target file system: {}", policy.getTargetDataset());
        String sourceDataset = policy.getSourceDataset();
        String targetDataSet = policy.getTargetDataset();
        if (FSUtils.isHCFS(new Path(sourceDataset))) {
            Cluster targetCluster = clusterDao.getActiveCluster(policy.getTargetCluster());
            FileSystem targetFS = FSUtils.getFileSystem(targetCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(targetCluster), false);
            Configuration conf = ClusterHelper.getHAConfigurationOrDefault(targetCluster);
            UserGroupInformation user = UserGroupInformation.getLoginUser();
            FSSnapshotUtils.createFSDirectory(targetFS, conf, FsPermission.getDirDefault(),
                    user.getShortUserName(), user.getShortUserName(), targetDataSet, false);
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
            FSSnapshotUtils.createFSDirectory(targetFS, conf, fsStatus.getPermission(),
                    fsStatus.getOwner(), fsStatus.getGroup(), targetDataSet, isSourceDirSnapshottable);
        } catch (IOException ioe) {
            LOG.error("Exception occurred while creating snapshottable directory on target: {}", ioe);
            throw new BeaconException("Exception occurred while creating snapshottable directory on target: ", ioe);
        }
    }
}
