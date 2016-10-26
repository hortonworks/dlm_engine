package com.hortonworks.beacon.client.entity;


import java.util.Arrays;

/**
 * Created by sramesh on 9/30/16.
 */
public enum EntityType {
    CLUSTER(Cluster.class, "name"),
    REPLICATIONPOLICY(ReplicationPolicy.class, "name");

    private final Class<? extends Entity> clazz;
    private String[] immutableProperties;

    private EntityType(Class<? extends Entity> typeClass, String... immutableProperties) {
        clazz = typeClass;
        this.immutableProperties = immutableProperties;
    }

    public Class<? extends Entity> getEntityClass() {
        return clazz;
    }

    public String[] getImmutableProperties() {
        return immutableProperties;
    }

    public boolean isSchedulable() {
        // Cluster is not schedulable like Policy
        return (this != EntityType.CLUSTER);
    }

    public static EntityType getEnum(String type) {
        try {
            return EntityType.valueOf(type.toUpperCase().trim());
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Invalid entity type: " + type + ". Expected "
                    + Arrays.toString(values()).toLowerCase() + ".");
        }
    }

}
