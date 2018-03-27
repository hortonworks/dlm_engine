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
