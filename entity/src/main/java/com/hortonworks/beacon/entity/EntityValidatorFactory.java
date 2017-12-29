/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

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
            case CLOUDCRED:
                return new CloudCredValidator();
            default:
                throw new IllegalArgumentException("Unhandled entity type: " + entityType);
        }
    }

}
