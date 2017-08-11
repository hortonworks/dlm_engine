/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.metrics;

import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.util.ReplicationType;

/**
 * Job Metrics handler to initialize the required concrete class for obtaining
 * job counters.
 */
public final class JobMetricsHandler {
    private JobMetricsHandler() {
    }

    public static JobMetrics getMetricsType(ReplicationType replType) {
        JobMetrics jobMetrics;
        switch (replType) {
            case FS:
                jobMetrics = new FSReplicationMetrics();
                break;
            case HIVE:
                jobMetrics = new HiveDRMetrics();
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.COMM_010009.name(), "Replication", replType.toString()));
        }
        return jobMetrics;
    }
}
