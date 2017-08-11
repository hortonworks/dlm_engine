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

/**
 * Base class that all Beacon resource class will extend.
 */
public abstract class Entity {

    private static final String EQUALS = "=";

    public abstract String getName();

    public abstract String getTags();

    /**
     * Enumeration of all possible status of an entity.
     */
    public enum EntityStatus {
        SUBMITTED, SUSPENDED, RUNNING, COMPLETED
    }

    public EntityType getEntityType() {
        for (EntityType type : EntityType.values()) {
            if (type.getEntityClass().equals(getClass())) {
                return type;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !o.getClass().equals(this.getClass())) {
            return false;
        }

        Entity entity = (Entity) o;

        String name = getName();
        return !(name != null ? !name.equals(entity.getName()) : entity.getName() != null);
    }

    @Override
    public int hashCode() {
        String clazz = this.getClass().getName();

        String name = getName();
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + clazz.hashCode();
        return result;
    }

    public String toShortString() {
        return "(" + getEntityType().name().toLowerCase() + ") " + getName();
    }

    void appendNonEmpty(StringBuilder policyDefinition, String name, Object field) {
        if (field != null) {
            policyDefinition.append(name).append(EQUALS).append(field).append(System.lineSeparator());
        }
    }
}
