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

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.util.HiveActionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Obtain and store Hive Replication counters.
 */
public class HiveReplicationMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(HiveReplicationMetrics.class);

    private Progress jobProgress = new Progress();

    public void obtainJobMetrics(JobContext  jobContext, List<String> queryLog, HiveActionType actionType)
            throws BeaconException {
        boolean isJobComplete = jobContext.getJobContextMap().containsKey(BeaconConstants.END_TIME);
        if (isJobComplete) {
            long startTime = Long.parseLong(jobContext.getJobContextMap().get(BeaconConstants.START_TIME));
            long endTime = Long.parseLong(jobContext.getJobContextMap().get(BeaconConstants.END_TIME));
            jobProgress.setTimeTaken(endTime - startTime);
        } else {
            long startTime = Long.parseLong(jobContext.getJobContextMap().get(BeaconConstants.START_TIME));
            jobProgress.setTimeTaken(System.currentTimeMillis() - startTime);
        }

        if (queryLog.size()!=0) {
            ParseHiveQueryLogV2 pq = new ParseHiveQueryLogV2();
            pq.parseQueryLog(queryLog, actionType);
            if (HiveActionType.EXPORT == actionType) {
                parseExportMetrics(jobContext, pq);
                loadExportMetrics(jobContext);
                loadProgressPercentage(jobContext, HiveActionType.EXPORT);
            } else {
                handleNoExportMetrics(jobContext, pq.getTotal());
                loadExportMetrics(jobContext);
                long importCompleted = pq.getCompleted();
                jobContext.getJobContextMap().put(ReplicationJobMetrics.IMPORT_COMPLETED.getName(), String.valueOf(
                        importCompleted));
                loadImportMetrics(jobContext);
                loadProgressPercentage(jobContext, HiveActionType.IMPORT);
            }
        }
        if (isJobComplete) {
            jobContext.getJobContextMap().put(ReplicationJobMetrics.IMPORT_COMPLETED.getName(),
                    jobContext.getJobContextMap().get(ReplicationJobMetrics.EXPORT_COMPLETED.getName()));
            loadExportMetrics(jobContext);
            loadImportMetrics(jobContext);
            loadProgressPercentage(jobContext, HiveActionType.IMPORT);
        }
    }

    private void handleNoExportMetrics(JobContext jobContext, long total) {
        if (!jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.EXPORT_TOTAL.getName())) {
            LOG.debug("No export metrics found!");
            jobContext.getJobContextMap().put(ReplicationJobMetrics.EXPORT_TOTAL.getName(), String.valueOf(
                    total));
            jobContext.getJobContextMap().put(ReplicationJobMetrics.EXPORT_COMPLETED.getName(), String.valueOf(
                    total));
        }
    }

    private void parseExportMetrics(JobContext jobContext, ParseHiveQueryLogV2 pq) {
        long exportTotal = 0L;
        if (!jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.EXPORT_TOTAL.getName())) {
            exportTotal = pq.getTotal();
        }
        long exportCompleted = pq.getCompleted();
        if (exportCompleted > exportTotal) {
            exportTotal = exportCompleted;
        }
        jobContext.getJobContextMap().put(ReplicationJobMetrics.EXPORT_TOTAL.getName(), String.valueOf(
                exportTotal));
        jobContext.getJobContextMap().put(ReplicationJobMetrics.EXPORT_COMPLETED.getName(), String.valueOf(
                exportCompleted));
    }

    private void loadProgressPercentage(JobContext jobContext, HiveActionType actionType) {
        long total, completed;
        float progress = 0;
        total = (Long.parseLong(jobContext.getJobContextMap().get(
                ReplicationJobMetrics.EXPORT_TOTAL.getName())));
        completed = (Long.parseLong(jobContext.getJobContextMap().get(
                ReplicationJobMetrics.EXPORT_COMPLETED.getName())));
        double exportProgress = Math.round(((float) completed/total) * 0.1 * 100.0);
        progress += exportProgress;
        LOG.debug("Export progress: total: {}, completed: {}, progress: {}", total, completed, exportProgress);
        if (actionType == HiveActionType.IMPORT) {
            completed = (Long.parseLong(jobContext.getJobContextMap().get(
                    ReplicationJobMetrics.IMPORT_COMPLETED.getName())));
            double importProgress = Math.round(((float) completed/total) * 0.9 * 100.0);
            LOG.debug("Import progress: total: {}, completed: {}, progress: {}", total, completed, importProgress);
            progress += importProgress;
            if (total == 0) {
                progress = 100.0f;
                jobContext.getJobContextMap().put(ReplicationJobMetrics.COMPLETED.getName(), String.valueOf(completed));
            }
        }
        progress = Math.round(progress * 100.0f)/100.0f;
        LOG.debug("Action Type: {}, Total: {}, Completed: {}, Progress: {}", actionType.getType(), total, completed,
                progress);
        jobProgress.setJobProgress(progress);
    }

    private void loadExportMetrics(JobContext jobContext) {
        long total = 0L;
        if (jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.EXPORT_TOTAL.getName())) {
            total = Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.EXPORT_TOTAL.getName()));
        }
        long importCompleted = 0;
        jobProgress.setTotal(total);
        jobProgress.setImportTotal(total);
        jobProgress.setExportTotal(total);
        jobProgress.setExportCompleted(
                Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.EXPORT_COMPLETED.getName())));
        if (jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.IMPORT_COMPLETED.getName())) {
            importCompleted = Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.IMPORT_COMPLETED
                    .getName()));
        }
        jobProgress.setCompleted(importCompleted);
        jobProgress.setImportCompleted(importCompleted);
    }

    private void loadImportMetrics(JobContext jobContext) {
        long importCompleted = 0L;
        if (jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.IMPORT_COMPLETED.getName())) {
            importCompleted = Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.IMPORT_COMPLETED
                    .getName()));
        } else {
            jobContext.getJobContextMap().put(ReplicationJobMetrics.IMPORT_COMPLETED.getName(), String.valueOf(0));
        }
        jobProgress.setCompleted(importCompleted);
        jobProgress.setImportCompleted(importCompleted);
    }

    public Progress getJobProgress() {
        return jobProgress;
    }
}
