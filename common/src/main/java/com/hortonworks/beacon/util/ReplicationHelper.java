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

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;

/**
 * Replication utility classes.
 */
public final class ReplicationHelper {
    private static final BeaconLog LOG = BeaconLog.getLog(ReplicationHelper.class);

    private ReplicationHelper() {
    }

    public static ReplicationType getReplicationType(String type) {
        try {
            return ReplicationType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException ex) {
            LOG.error(MessageCode.COMM_000024.name(), type);
            throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.COMM_000014.name(), type));
        }
    }

}
