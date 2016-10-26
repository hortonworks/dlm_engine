package com.hortonworks.beacon.entity;


import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterValidator extends EntityValidator<Cluster> {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterValidator.class);

    public ClusterValidator() {
        super(EntityType.CLUSTER);
    }

    @Override
    public void validate(Cluster entity) throws BeaconException {
    // Do any validation here
    }
}