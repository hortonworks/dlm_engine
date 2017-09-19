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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.util.HiveActionType;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;

/**
 * Obtain and store Hive Replication counters.
 */
public class HiveReplicationMetrics extends JobMetrics {

    HiveReplicationMetrics() {
        super();
    }

    protected void collectJobMetrics(Job job) throws IOException, InterruptedException {
        populateReplicationCountersMap(job);
    }

    public void obtainJobMetrics(JobContext jobContext, List<String> queryLog, HiveActionType actionType)
            throws BeaconException {
        if (queryLog.size()!=0) {
            long total = 0L;
            long completed = 0L;
            ParseHiveQueryLog pq = (new ParseHiveQueryLog()).parseQueryLog(queryLog, actionType);
            if (HiveActionType.EXPORT == actionType) {
                if (!jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.TOTAL.getName())) {
                    total = pq.getTotal();
                } else {
                    total += (Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.TOTAL.getName())));
                }
                getMetricsMap().put(ReplicationJobMetrics.TOTAL.getName(), total);
                getMetricsMap().put(ReplicationJobMetrics.COMPLETED.getName(), completed);
                jobContext.getJobContextMap().put(ReplicationJobMetrics.TOTAL.getName(), String.valueOf(total));
            } else {
                getMetricsMap().put(ReplicationJobMetrics.TOTAL.getName(),
                        Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.TOTAL.getName())));
                if (!jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.COMPLETED.getName())) {
                    completed = pq.getCompleted();
                } else {
                    completed += (Long.parseLong(jobContext.getJobContextMap().get(
                            ReplicationJobMetrics.COMPLETED.getName())));
                }
                getMetricsMap().put(ReplicationJobMetrics.COMPLETED.getName(), completed);
                jobContext.getJobContextMap().put(ReplicationJobMetrics.COMPLETED.getName(), String.valueOf(completed));
            }
        }
    }
}
