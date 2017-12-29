/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client.entity;


import java.util.Arrays;

import com.hortonworks.beacon.util.StringFormat;

/**
 * Enum for Beacon resource type.
 */
public enum EntityType {
    CLUSTER(Cluster.class, "name"),
    REPLICATIONPOLICY(ReplicationPolicy.class, "name"),
    CLOUDCRED(CloudCred.class, "name");

    private final Class<? extends Entity> clazz;
    private String[] immutableProperties;

    EntityType(Class<? extends Entity> typeClass, String... immutableProperties) {
        clazz = typeClass;
        this.immutableProperties = immutableProperties;
    }

    public Class<? extends Entity> getEntityClass() {
        return clazz;
    }

    public String[] getImmutableProperties() {
        return Arrays.copyOf(immutableProperties, immutableProperties.length);
    }

    public static EntityType getEnum(String type) {
        try {
            return EntityType.valueOf(type.toUpperCase().trim());
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException(StringFormat.format("Invalid entity type: {}. Expected {}.", type,
                Arrays.toString(values()).toLowerCase()));
        }
    }

}
