package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;

public final class PolicyHelper {
    private PolicyHelper() {
    }

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
}