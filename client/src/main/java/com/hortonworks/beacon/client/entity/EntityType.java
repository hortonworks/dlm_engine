/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
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
