package com.hortonworks.beacon.api.util;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;

import javax.ws.rs.core.Response;

public final class ValidationUtil {
    private static final String ERROR_MESSAGE_PART1 = "This operation is not allowed on source cluster: ";
    private static final String ERROR_MESSAGE_PART2 = ".Try it on target cluster: ";
    private static final BeaconConfig config = BeaconConfig.getInstance();
    private static final ConfigurationStore configStore = ConfigurationStore.getInstance();

    private ValidationUtil() {
    }

    public static void validateIfAPIRequestAllowed(String replicationPolicyName)
            throws BeaconException {

        ReplicationPolicy policy = configStore.getEntity(EntityType.REPLICATIONPOLICY, replicationPolicyName);
        if (policy == null) {
            throw BeaconWebException.newAPIException(replicationPolicyName + " (" + EntityType.REPLICATIONPOLICY.name() +
                    ") not " + "found", Response.Status.NOT_FOUND);
        }

        isRequestAllowed(policy);
    }

    public static void validateIfAPIRequestAllowed(ReplicationPolicy policy)
            throws BeaconException {
        if (policy == null) {
            throw new BeaconException("Policy cannot be null");
        }

        isRequestAllowed(policy);
    }

    private static void isRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = config.getEngine().getLocalClusterName();

        // If policy is HCFS then requests are allowed on source cluster
        if (localClusterName.equalsIgnoreCase(sourceClusterName) &&
                !PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            String message = ERROR_MESSAGE_PART1 + sourceClusterName + ERROR_MESSAGE_PART2 + targetClusterName;
            throw BeaconWebException.newAPIException(message);
        }
    }
}
