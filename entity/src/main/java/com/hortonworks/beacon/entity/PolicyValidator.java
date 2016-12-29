package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PolicyValidator extends EntityValidator<ReplicationPolicy> {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyValidator.class);

    public PolicyValidator() {
        super(EntityType.REPLICATIONPOLICY);
    }

    @Override
    public void validate(ReplicationPolicy entity) throws BeaconException {
        validateEntityExists(EntityType.CLUSTER, entity.getSourceCluster());
        validateEntityExists(EntityType.CLUSTER, entity.getTargetCluster());

        validateIfClustersPaired(EntityType.CLUSTER, entity.getSourceCluster(), entity.getTargetCluster());
    }

    private static void validateEntityExists(EntityType type, String name) throws BeaconException {
        if (ConfigurationStore.getInstance().getEntity(type, name) == null) {
            throw new ValidationException("Referenced " + type + " " + name + " is not registered. Source and target " +
                    "clusters in the policy should be paired before submitting or scheduling the policy");
        }
    }

    private static void validateIfClustersPaired(EntityType type, String sourceClluster,
                                                 String targetCluster) throws BeaconException {
        boolean paired = false;
        String[] peers = ClusterHelper.getPeers(sourceClluster);
        if (peers != null && peers.length > 0) {
            for (String peer : peers) {
                if (peer.trim().equalsIgnoreCase(targetCluster)) {
                    paired = true;
                }
            }
        }

        peers = ClusterHelper.getPeers(targetCluster);
        if (peers != null && peers.length > 0) {
            for (String peer : peers) {
                if (peer.trim().equalsIgnoreCase(sourceClluster)) {
                    paired = true;
                }
            }
        }

        if (!paired) {
            throw new ValidationException("Clusters " + sourceClluster + " and " + targetCluster + " are not paired. " +
                    "Pair the clusters before submitting or scheduling the policy");
        }
    }
}
