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


import java.util.Arrays;

/**
 * Enum for Beacon resource type.
 */
public enum EntityType {
    CLUSTER(Cluster.class, "name"),
    REPLICATIONPOLICY(ReplicationPolicy.class, "name");

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
            throw new IllegalArgumentException("Invalid entity type: " + type + ". Expected "
                    + Arrays.toString(values()).toLowerCase() + ".");
        }
    }

}
