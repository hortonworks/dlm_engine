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
import com.hortonworks.beacon.replication.hive.HivePolicyHelper;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
        String executionType = policy.getExecutionType();
        if (executionType.equalsIgnoreCase(FS_SNAPSHOT)) {
            String clusterName = policy.getTargetCluster();
            String targetDataset = policy.getTargetDataset();
            Cluster cluster = ClusterPersistenceHelper.getActiveCluster(clusterName);
            try(FileSystem fileSystem = FSUtils.getFileSystem(cluster.getFsEndpoint(), new Configuration(), false)) {
                RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(targetDataset), true);
                if (files != null && files.hasNext()) {
                    throw new ValidationException(MessageCode.MAIN_000152.name());
                }
            } catch (IOException e) {
                throw new BeaconException(e);
            }
        }
    }

    private static void validateDBTargetDS(ReplicationPolicy policy) throws BeaconException {
        Cluster cluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
        String targetDataset = policy.getTargetDataset();
        String hsEndPoint = cluster.getHsEndpoint();
        HiveDRUtils.initializeDriveClass();
        String connString = HiveDRUtils.getHS2ConnectionUrl(hsEndPoint, targetDataset);
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
                            throw new SQLException(MessageCode.MAIN_000153.getMsg(), targetDataset);
                        }
                    }
                }
            }
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
}
