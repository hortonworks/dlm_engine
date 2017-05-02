/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.client.entity;

/**
 * Base class that all Beacon resource class will extend.
 */
public abstract class Entity {

    private static final String EQUALS = "=";

    public abstract String getName();

    public abstract String getTags();

    public abstract Acl getAcl();

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
