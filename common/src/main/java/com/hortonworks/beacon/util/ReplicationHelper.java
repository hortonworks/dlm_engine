/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replication utility classes.
 */
public final class ReplicationHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationHelper.class);

    private ReplicationHelper() {
    }

    public static ReplicationType getReplicationType(String type) {
        try {
            return ReplicationType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException ex) {
            LOG.error("{} is not a valid replication type", type);
            throw new IllegalArgumentException(
                StringFormat.format("Policy of Replication type ({}) is not supported ", type));
        }
    }

}
