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

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Obtain and store Hive Replication counters.
 */
public class HiveDRMetrics extends JobMetrics {

    HiveDRMetrics() {
        super();
    }

    protected void collectJobMetrics(Job job) throws IOException, InterruptedException {
        populateReplicationCountersMap(job);
    }
}
