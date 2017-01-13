package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PolicyHelper {
    private PolicyHelper() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(PolicyHelper.class);

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
}