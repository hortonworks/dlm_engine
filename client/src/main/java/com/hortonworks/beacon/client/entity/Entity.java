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

import com.hortonworks.beacon.api.PropertiesIgnoreCase;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.List;

/**
 * Base class that all Beacon resource class will extend.
 */
public abstract class Entity {

    private static final String EQUALS = "=";
    private final EntityType entityType;

    public abstract String getName();

    public abstract List<String> getTags();

    public abstract PropertiesIgnoreCase asProperties();

    /**
     * Enumeration of all possible status of an entity.
     */
    public enum EntityStatus {
        SUBMITTED, SUSPENDED, RUNNING, COMPLETED, SUCCEEDED, SUCCEEDEDWITHSKIPPED, FAILEDWITHSKIPPED,
        SUSPENDEDFORINTERVENTION
    }

    public Entity(EntityType entityType) {
        this.entityType = entityType;
    }

    @JsonIgnore
    public EntityType getEntityType() {
        return entityType;
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
