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

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.ClusterValidator;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.ClusterPersistenceHelper;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Utility class to validate API requests.
 */
public final class ValidationUtil {
    private static final BeaconLog LOG = BeaconLog.getLog(ValidationUtil.class);
    private static final String FS_SNAPSHOT = "FS_SNAPSHOT";
    private static final String SHOW_DATABASES = "SHOW DATABASES";
    private static final String SHOW_TABLES = "SHOW TABLES";
    private static final String USE = "USE ";

    private ValidationUtil() {
    }

    public static void validationOnSubmission(ReplicationPolicy replicationPolicy) throws BeaconException {
        validateIfAPIRequestAllowed(replicationPolicy);
        validatePolicy(replicationPolicy);
        validateEntityDataset(replicationPolicy);
    }

    public static void validateIfAPIRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        if (policy == null) {
            throw new BeaconException(MessageCode.COMM_010008.name(), "Policy");
        }

        isRequestAllowed(policy);
    }

    private static void isRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = ClusterHelper.getLocalCluster().getName();

        // If policy is HCFS then requests are allowed on source cluster
        if (localClusterName.equalsIgnoreCase(sourceClusterName)
                && !PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000005.name(), sourceClusterName,
                    targetClusterName);
        }
    }

    private static void validatePolicy(final ReplicationPolicy policy) throws BeaconException {
        ReplicationType replType = ReplicationHelper.getReplicationType(policy.getType());
        switch (replType) {
            case FS:
                FSPolicyHelper.validateFSReplicationProperties(FSPolicyHelper.buildFSReplicationProperties(policy));
                validateFSTargetDS(policy);
                break;
            case HIVE:
                HivePolicyHelper.validateHiveReplicationProperties(
                        HivePolicyHelper.buildHiveReplicationProperties(policy));
                validateDBTargetDS(policy);
                break;
            default:
                throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.COMM_010007.name(), policy.getType()));
        }
    }

    private static void validateEntityDataset(final ReplicationPolicy policy) throws BeaconException {
        checkSameDataset(policy);
        checkDatasetConfliction(policy);
    }

    private static void checkSameDataset(ReplicationPolicy policy) throws BeaconException {
        String sourceDataset = policy.getSourceDataset();
        String targetDataset = policy.getTargetDataset();

        if (!targetDataset.equals(sourceDataset)) {
            LOG.error(MessageCode.MAIN_000031.name(), targetDataset, sourceDataset);
            throw new BeaconException(MessageCode.MAIN_000031.name(), targetDataset, sourceDataset);
        }
    }

    private static void checkDatasetConfliction(ReplicationPolicy policy) throws BeaconException {
        boolean isConflicted = ReplicationUtils.isDatasetConflicting(
                ReplicationHelper.getReplicationType(policy.getType()), policy.getSourceDataset());
        if (isConflicted) {
            LOG.error(MessageCode.MAIN_000032.name(), policy.getSourceDataset());
            throw new BeaconException(MessageCode.MAIN_000032.name(), policy.getSourceDataset());
        }
    }

    private static void validateFSTargetDS(ReplicationPolicy policy) throws BeaconException {
        String clusterName = policy.getTargetCluster();
        String targetDataset = policy.getTargetDataset();
        Cluster cluster = ClusterPersistenceHelper.getActiveCluster(clusterName);
        try {
            FileSystem fileSystem = FSUtils.getFileSystem(cluster.getFsEndpoint(), new Configuration(), false);
            if (fileSystem.exists(new Path(targetDataset))) {
                RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(targetDataset), true);
                if (files != null && files.hasNext()) {
                    throw new ValidationException(MessageCode.MAIN_000152.name(), targetDataset);
                }
            } else {
                createFSSnapshotDirectory(policy);
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
        String connString = HiveDRUtils.getHS2ConnectionUrl(hsEndPoint, " ");
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
                            throw new ValidationException(MessageCode.MAIN_000153.getMsg(), targetDataset);
                        }
                    }
                }
            }
        } catch (ValidationException e) {
            throw e;
        } catch (Exception sqe) {
            LOG.error(MessageCode.ENTI_000014.name(), sqe.getMessage());
            throw new ValidationException(MessageCode.ENTI_000014.name(), sqe.getMessage());
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

    private static void createFSSnapshotDirectory(ReplicationPolicy policy) throws BeaconException {
        LOG.info(MessageCode.MAIN_000156.name(), policy.getTargetDataset());
        Cluster sourceCluster = ClusterPersistenceHelper.getActiveCluster(policy.getSourceCluster());
        Cluster targetCluster = ClusterPersistenceHelper.getActiveCluster(policy.getTargetCluster());
        String sourceDataset = FSUtils.getStagingUri(sourceCluster.getFsEndpoint(), policy.getSourceDataset());
        String targetDataSet = FSUtils.getStagingUri(targetCluster.getFsEndpoint(), policy.getTargetDataset());

        try {
            FileSystem sourceFS = FSUtils.getFileSystem(sourceCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(sourceCluster), false);
            FileSystem targetFS = FSUtils.getFileSystem(targetCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(targetCluster), false);

            boolean isSourceDirSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(sourceFS, sourceDataset);
            LOG.info(MessageCode.MAIN_000158.name(), sourceDataset, isSourceDirSnapshottable);
            if (isSourceDirSnapshottable) {
                FileStatus fsStatus = sourceFS.getFileStatus(new Path(sourceDataset));
                Configuration conf = ClusterHelper.getHAConfigurationOrDefault(targetCluster);
                conf.set(ClusterValidator.FS_DEFAULT_NAME_KEY, targetCluster.getFsEndpoint());
                FSSnapshotUtils.createSnapShotDirectory(targetFS, conf, fsStatus.getPermission(),
                        fsStatus.getOwner(), fsStatus.getGroup(), targetDataSet);
            }
        } catch (IOException ioe) {
            LOG.error(MessageCode.MAIN_000157.getMsg(), ioe);
            throw new BeaconException(MessageCode.MAIN_000157.name(), ioe.getMessage());
        }
    }
}
