package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PolicyHelper {
    private PolicyHelper() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(PolicyHelper.class);
    public static final String INSTANCE_EXECUTION_TYPE = "INSTANCE_EXECUTION_TYPE";

    public static String getRemoteBeaconEndpoint(final String policyName) throws BeaconException {
        String remoteClusterName = getRemoteClusterName(policyName);
        Cluster remoteCluster = EntityHelper.getEntity(EntityType.CLUSTER, remoteClusterName);
        return remoteCluster.getBeaconEndpoint();
    }

    public static String getRemoteClusterName(final String policyName) throws BeaconException {
        String localClusterName = BeaconConfig.getInstance().getEngine().getLocalClusterName();
        ReplicationPolicy policy = EntityHelper.getEntity(EntityType.REPLICATIONPOLICY, policyName);
        String remoteClusterName = policy.getSourceCluster().equalsIgnoreCase(localClusterName)
                ? policy.getTargetCluster() : policy.getSourceCluster();
        return remoteClusterName;
    }

    public static boolean isPolicyHCFS(final String sourceDataset, final String targetDataset) throws BeaconException {
        if (StringUtils.isNotBlank(sourceDataset)) {
            Path sourceDatasetPath = new Path(sourceDataset);
            if (FSUtils.isHCFS(sourceDatasetPath)) {
                return true;
            }
        }

        if (StringUtils.isNotBlank(targetDataset)) {
            Path targetDatasetPath = new Path(targetDataset);
            if (FSUtils.isHCFS(targetDatasetPath)) {
                return true;
            }
        }

        return false;
    }


    public static String getReplicationPolicyType(ReplicationPolicy policy) throws BeaconException {
        String policyType = policy.getType().toUpperCase();

        Cluster sourceCluster;
        Cluster targetCluster;

        if (policyType.equalsIgnoreCase(ReplicationType.FS.getName())) {
            DistributedFileSystem sourceFs = null;
            DistributedFileSystem targetFs = null;
            boolean isSnapshot = false;
            boolean tdeEncryptionEnabled = Boolean.parseBoolean(
                    policy.getCustomProperties().getProperty((FSUtils.TDE_ENCRYPTION_ENABLED), "false"));

            try {
                sourceCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getSourceCluster());
                targetCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getTargetCluster());
                sourceFs = FSUtils.getFileSystem(sourceCluster.getFsEndpoint(), new Configuration());
                targetFs = FSUtils.getFileSystem(targetCluster.getFsEndpoint(), new Configuration());
                String sourceDataset = sourceCluster.getFsEndpoint() + policy.getSourceDataset();
                String targetDataset = targetCluster.getFsEndpoint() + policy.getTargetDataset();
                isSnapshot = FSUtils.isDirectorySnapshottable(sourceFs, targetFs, sourceDataset, targetDataset);

                if (!tdeEncryptionEnabled && isSnapshot) {
                    policyType = ReplicationType.FS+"_SNAPSHOT";
                }

            } catch (BeaconException e) {
                LOG.error("Unable to get Policy details ", e);
                throw new BeaconException(e);
            }
        }

        LOG.info("PolicyType {} obtained for entity : {}", policyType, policy.getName());

        return policyType;
    }
}