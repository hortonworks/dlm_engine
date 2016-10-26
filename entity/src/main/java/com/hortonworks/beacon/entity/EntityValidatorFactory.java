package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.EntityType;

/**
 * Factory Class which returns the Parser based on the EntityType.
 */
public final class EntityValidatorFactory {

    private EntityValidatorFactory() {
    }

    /**
     *
     * @param entityType - entity type
     * @return concrete parser based on entity type
     */
    public static EntityValidator getValidator(final EntityType entityType) {

        switch (entityType) {
            case CLUSTER:
                return new ClusterValidator();
            case REPLICATIONPOLICY:
                return new PolicyValidator();
            default:
                throw new IllegalArgumentException("Unhandled entity type: " + entityType);
        }
    }

}