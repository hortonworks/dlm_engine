package com.hortonworks.beacon.entity;

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
    }
}
