package com.hortonworks.beacon.entity;


import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EntityValidator <T extends Entity> {
    private static final Logger LOG = LoggerFactory.getLogger(EntityValidator.class);

    private final EntityType entityType;

    public EntityValidator(EntityType entityType) {
        this.entityType = entityType;
    }

    public EntityType getEntityType() {
        return this.entityType;
    }

    public abstract void validate(T entity) throws BeaconException;
}
