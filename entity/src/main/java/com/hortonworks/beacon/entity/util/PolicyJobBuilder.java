package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.entity.Cluster;
import com.hortonworks.beacon.entity.Entity;
import com.hortonworks.beacon.entity.EntityType;
import com.hortonworks.beacon.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationType;
import com.hortonworks.beacon.replication.hdfs.HDFSDRProperties;
import com.hortonworks.beacon.replication.hdfs.HDFSReplicationJobDetails;
import com.hortonworks.beacon.replication.hdfssnapshot.HDFSSnapshotDRProperties;
import com.hortonworks.beacon.replication.hdfssnapshot.HDFSSnapshotReplicationJobDetails;
import com.hortonworks.beacon.replication.hive.HiveDRProperties;
import com.hortonworks.beacon.replication.hive.HiveReplicationJobDetails;
import com.hortonworks.beacon.util.DateUtil;

import java.util.Properties;

public class PolicyJobBuilder {
    public static ReplicationJobDetails buildReplicationJob(Entity entity) throws BeaconException {
        ReplicationPolicy policy = (ReplicationPolicy) entity;
        ReplicationType type = ReplicationType.valueOf(policy.getType());
        ReplicationJobDetails job = null;
        switch (type) {
            case HDFS:
                job = createHDFSJob(policy);
                break;
            case HDFSSNAPSHOT:
                job = createHDFSSnapshotJob(policy);
                break;
            case HIVE:
                job = createHiveJob(policy);
                break;
        }
        return job;
    }

    private static ReplicationJobDetails createHiveJob(ReplicationPolicy policy) throws BeaconException {
        HiveReplicationJobDetails job = new HiveReplicationJobDetails();
        EntityType type = policy.getEntityType();
        Cluster sourceCluster = getCluster(type, policy.getSourceCluster());
        Cluster targetCluster = getCluster(type, policy.getTargetCluster());
        Properties customProp = policy.getCustomProperties();
        Properties prop = new Properties();
        prop.setProperty(HiveDRProperties.JOB_NAME.getName(), policy.getName());
        prop.setProperty(HiveDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        prop.setProperty(HiveDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        prop.setProperty(HiveDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));
        prop.setProperty(HiveDRProperties.SOURCE_HS2_URI.getName(), sourceCluster.getHsEndpoint());
        prop.setProperty(HiveDRProperties.SOURCE_DATABASE.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_DATABASE.getName()));
        prop.setProperty(HiveDRProperties.SOURCE_TABLES.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_TABLES.getName()));
        prop.setProperty(HiveDRProperties.STAGING_PATH.getName(),
                customProp.getProperty(HiveDRProperties.STAGING_PATH.getName()));
        prop.setProperty(HiveDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
        prop.setProperty(HiveDRProperties.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName()));
        prop.setProperty(HiveDRProperties.TARGET_HS2_URI.getName(), targetCluster.getHsEndpoint());
        prop.setProperty(HiveDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());
        prop.setProperty(HiveDRProperties.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HiveDRProperties.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName()));
        prop.setProperty(HiveDRProperties.MAX_EVENTS.getName(),
                customProp.getProperty(HiveDRProperties.MAX_EVENTS.getName()));
        prop.setProperty(HiveDRProperties.REPLICATION_MAX_MAPS.getName(),
                customProp.getProperty(HiveDRProperties.REPLICATION_MAX_MAPS.getName()));
        prop.setProperty(HiveDRProperties.DISTCP_MAX_MAPS.getName(),
                customProp.getProperty(HiveDRProperties.DISTCP_MAX_MAPS.getName()));
        prop.setProperty(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                customProp.getProperty(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
        prop.setProperty(HiveDRProperties.DISTCP_MAP_BANDWIDTH.getName(),
                customProp.getProperty(HiveDRProperties.DISTCP_MAP_BANDWIDTH.getName()));
        job.validateReplicationProperties(prop);
        job = job.setReplicationJobDetails(prop);
        return job;
    }

    private static ReplicationJobDetails createHDFSSnapshotJob(ReplicationPolicy policy) throws BeaconException {
        HDFSSnapshotReplicationJobDetails job = new HDFSSnapshotReplicationJobDetails();
        EntityType type = policy.getEntityType();
        Cluster sourceCluster = getCluster(type, policy.getSourceCluster());
        Cluster targetCluster = getCluster(type, policy.getTargetCluster());
        Properties customProp = policy.getCustomProperties();
        Properties prop = new Properties();
        prop.setProperty(HDFSSnapshotDRProperties.JOB_NAME.getName(), policy.getName());
        prop.setProperty(HDFSSnapshotDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        prop.setProperty(HDFSSnapshotDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        prop.setProperty(HDFSSnapshotDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));
        prop.setProperty(HDFSSnapshotDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
        prop.setProperty(HDFSSnapshotDRProperties.SOURCE_EXEC_URL.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.SOURCE_EXEC_URL.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.SOURCE_NN_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.SOURCE_NN_KERBEROS_PRINCIPAL.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_DIR.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_DIR.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());
        prop.setProperty(HDFSSnapshotDRProperties.TARGET_EXEC_URL.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TARGET_EXEC_URL.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.TARGET_NN_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TARGET_NN_KERBEROS_PRINCIPAL.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_DIR.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_DIR.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.DISTCP_MAX_MAPS.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.DISTCP_MAX_MAPS.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()));
        prop.setProperty(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()));

        prop.setProperty(HDFSSnapshotDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
        job.validateReplicationProperties(prop);
        job = job.setReplicationJobDetails(prop);
        return job;
    }

    private static ReplicationJobDetails createHDFSJob(ReplicationPolicy policy) throws BeaconException {
        HDFSReplicationJobDetails job = new HDFSReplicationJobDetails();
        EntityType type = policy.getEntityType();
        Cluster sourceCluster = getCluster(type, policy.getSourceCluster());
        Cluster targetCluster = getCluster(type, policy.getTargetCluster());
        Properties customProp = policy.getCustomProperties();
        Properties prop = new Properties();
        prop.setProperty(HDFSDRProperties.JOB_NAME.getName(), policy.getName());
        prop.setProperty(HDFSDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        prop.setProperty(HDFSDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        prop.setProperty(HDFSDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));
        prop.setProperty(HDFSDRProperties.SOURCE_DIR.getName(),
                customProp.getProperty(HDFSDRProperties.SOURCE_DIR.getName()));
        prop.setProperty(HDFSDRProperties.SOURCE_CLUSTER_FS_READ_ENDPOINT.getName(), sourceCluster.getFsEndpoint());
        prop.setProperty(HDFSDRProperties.TARGET_DIR.getName(),
                customProp.getProperty(HDFSDRProperties.TARGET_DIR.getName()));
        prop.setProperty(HDFSDRProperties.TARGET_CLUSTER_FS_WRITE_ENDPOINT.getName(), targetCluster.getFsEndpoint());
        prop.setProperty(HDFSDRProperties.DISTCP_MAX_MAPS.getName(),
                customProp.getProperty(HDFSDRProperties.DISTCP_MAX_MAPS.getName()));
        prop.setProperty(HDFSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(),
                customProp.getProperty(HDFSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName()));
        job.validateReplicationProperties(prop);
        job = job.setReplicationJobDetails(prop);
        return job;
    }

    private static Cluster getCluster(EntityType type, String name) throws BeaconException {
        ConfigurationStore store = ConfigurationStore.get();
        Entity entity = store.get(type, name);
        return (Cluster) entity;
    }
}
