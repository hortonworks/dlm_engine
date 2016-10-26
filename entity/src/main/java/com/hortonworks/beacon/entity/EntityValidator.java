package com.hortonworks.beacon.entity;


import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
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

    protected void validateEntityExists(EntityType type, String name) throws BeaconException {
        if (ConfigurationStore.get().get(type, name) == null) {
            throw new ValidationException("Referenced " + type + " " + name + " is not registered");
        }
    }

    public abstract void validate(T entity) throws BeaconException;
}
