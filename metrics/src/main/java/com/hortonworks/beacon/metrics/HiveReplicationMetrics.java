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

import static com.hortonworks.beacon.constants.BeaconConstants.DATABASE_BOOTSTRAP;
import static com.hortonworks.beacon.constants.BeaconConstants.DATASET_BOOTSTRAP;

/**
 * Obtain and store Hive Replication counters.
 */
public class HiveReplicationMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(HiveReplicationMetrics.class);

    private Progress jobProgress = new Progress();

    private static final float BOOTSTRAP_WEIGHTAGE = 0.9f;
    private static final float INCREMENTAL_WEIGHTAGE = (1 - BOOTSTRAP_WEIGHTAGE);

    public void obtainJobMetrics(JobContext  jobContext, List<String> queryLog, HiveActionType actionType)
            throws BeaconException {
        boolean isDatasetBootstrap = Boolean.valueOf(jobContext.getJobContextMap().get(DATASET_BOOTSTRAP));
        boolean isDatabaseBootstrap = Boolean.valueOf(jobContext.getJobContextMap().get(DATABASE_BOOTSTRAP));
        boolean restoreTableLevelMetrics = false;
        JobContext databaseBootstrapJobContext = null;
        if (isDatasetBootstrap && !isDatabaseBootstrap) {
            databaseBootstrapJobContext = new JobContext(jobContext);
            restoreTableLevelMetrics = true;
        }

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
            printHiveQueryLog(queryLog);
            pq.parseQueryLog(queryLog, actionType);
            if (HiveActionType.EXPORT == actionType) {
                parseExportMetrics(jobContext, pq);
                loadExportMetrics(jobContext);
                loadProgressPercentage(jobContext, HiveActionType.EXPORT, pq);
            } else {
                updateImportMetrics(jobContext, pq);
            }
        }
        if (isJobComplete) {
            jobContext.getJobContextMap().put(ReplicationJobMetrics.IMPORT_COMPLETED.getName(),
                    jobContext.getJobContextMap().get(ReplicationJobMetrics.EXPORT_COMPLETED.getName()));
            loadExportMetrics(jobContext);
            loadImportMetrics(jobContext);
            loadProgressPercentage(jobContext, HiveActionType.IMPORT, null);
            jobProgress.setJobProgress(100);
        }
        if (restoreTableLevelMetrics) {
            restoreJobContext(jobContext, databaseBootstrapJobContext);
        }
    }

    private void printHiveQueryLog(List<String> queryLogList) {
        for (String queryLogMessage: queryLogList) {
            LOG.info(queryLogMessage);
        }
    }

    private void updateImportMetrics(JobContext jobContext, ParseHiveQueryLogV2 pq) {
        handleNoExportMetrics(jobContext, pq.getTotal());
        loadExportMetrics(jobContext);
        long importTotal = pq.getTotal();
        long importCompleted = pq.getCompleted();
        jobContext.getJobContextMap().put(ReplicationJobMetrics.IMPORT_TOTAL.getName(), String.valueOf(
                importTotal));
        jobContext.getJobContextMap().put(ReplicationJobMetrics.IMPORT_COMPLETED.getName(), String.valueOf(
                importCompleted));
        loadImportMetrics(jobContext);
        loadProgressPercentage(jobContext, HiveActionType.IMPORT, pq);
    }

    private void handleNoExportMetrics(JobContext jobContext, long total) {
        if (!jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.EXPORT_TOTAL.getName())) {
            LOG.debug("No export metrics found!");
            jobContext.getJobContextMap().put(ReplicationJobMetrics.EXPORT_TOTAL.getName(), String.valueOf(total));
            jobContext.getJobContextMap().put(ReplicationJobMetrics.EXPORT_COMPLETED.getName(), String.valueOf(total));
            jobContext.getJobContextMap().put(ReplicationJobMetrics.IMPORT_TOTAL.getName(), String.valueOf(total));
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

    private void loadProgressPercentage(JobContext jobContext, HiveActionType actionType, ParseHiveQueryLogV2 pq) {
        long total, completed;
        float progress = 0;
        float exportProgress = 10;
        if (jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.EXPORT_TOTAL.getName())) {
            total = (Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.EXPORT_TOTAL.getName())));
            completed = (Long.parseLong(jobContext.getJobContextMap().get(
                    ReplicationJobMetrics.EXPORT_COMPLETED.getName())));
            exportProgress = Math.min(10, ((float) completed/total) * 0.1f * 100.0f);
            LOG.debug("Export progress: total: {}, completed: {}, progress: {}", total, completed, exportProgress);
        }
        progress += exportProgress;
        if (actionType == HiveActionType.IMPORT) {
            total = Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.IMPORT_TOTAL.getName()));
            completed = (Long.parseLong(jobContext.getJobContextMap().get(
                    ReplicationJobMetrics.IMPORT_COMPLETED.getName())));
            float importProgress = Math.min(90, ((float) completed/total) * 0.9f * 100.0f);
            LOG.debug("Import progress: total: {}, completed: {}, progress: {}", total, completed, importProgress);
            progress += importProgress;
        }
        boolean isDatabaseBootstrap = Boolean.valueOf(jobContext.getJobContextMap().get(DATABASE_BOOTSTRAP));
        boolean isDatasetBootstrap = Boolean.valueOf(jobContext.getJobContextMap().get(DATASET_BOOTSTRAP));
        progress = getProgressWithAppliedWeightage(isDatasetBootstrap, isDatabaseBootstrap, pq != null, progress);
        progress = Math.min(100, Math.round(progress * 100.0f)/100.0f);
        LOG.debug("Action Type: {}, Progress: {}", actionType.getType(), progress);
        jobProgress.setJobProgress(progress);
    }

    private void loadExportMetrics(JobContext jobContext) {
        long total = 0L;
        if (jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.EXPORT_TOTAL.getName())) {
            total = Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.EXPORT_TOTAL.getName()));
        }
        jobProgress.setTotal(total);
        jobProgress.setImportTotal(total);
        jobProgress.setExportTotal(total);
        jobProgress.setExportCompleted(
                Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.EXPORT_COMPLETED.getName())));
    }

    private void loadImportMetrics(JobContext jobContext) {
        long importCompleted = 0L;
        if (jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.IMPORT_TOTAL.getName())) {
            long importTotal = Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.IMPORT_TOTAL
                    .getName()));
            jobProgress.setImportTotal(importTotal);
            jobProgress.setTotal(importTotal);
        } else {
            if (jobContext.getJobContextMap().containsKey(ReplicationJobMetrics.EXPORT_TOTAL.getName())) {
                long exportTotal = Long.parseLong(jobContext.getJobContextMap().get(ReplicationJobMetrics.EXPORT_TOTAL
                        .getName()));
                jobContext.getJobContextMap().put(ReplicationJobMetrics.IMPORT_TOTAL.getName(),
                        String.valueOf(exportTotal));
                jobProgress.setImportTotal(exportTotal);
                jobProgress.setTotal(exportTotal);
            }
        }
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

    /**
     * This method computes the progress percentage based on the weightage of the run.
     * @param isDatasetBootstrap If the run is dataset bootstrap run.
     * @param isDatabaseBootstrap If it is database bootstrap phase of the run.
     * @param metricsAvailable If metrics was successfully derived from query logs.
     * @param progress The progess computed from query logs.
     * @return The final progress after considering the weightage of the run.
     */
    private float getProgressWithAppliedWeightage(boolean isDatasetBootstrap, boolean isDatabaseBootstrap,
                                                  boolean metricsAvailable, float progress) {
        if (metricsAvailable) {
            if (isDatasetBootstrap && isDatabaseBootstrap) {
                progress = BOOTSTRAP_WEIGHTAGE * progress;
            } else if (isDatasetBootstrap) {
                progress = (BOOTSTRAP_WEIGHTAGE * 100) + (INCREMENTAL_WEIGHTAGE * progress);
            } else {
                LOG.debug("Post dataset incremental replication. Progress: {}", progress);
            }
        } else {
            if (isDatabaseBootstrap) {
                progress = BOOTSTRAP_WEIGHTAGE * 100;
            } else {
                progress = 100;
            }
        }
        return progress;
    }

    /**
     * This method restores the metrics computed in incremental phase to bootstrap phase except the progress
     * percentage.
     * @param jobContext jobContext during incremental phase, events metrics.
     * @param databaseBootstrapJobContext jobContext during bootstrap phase, table metrics.
     */
    private void restoreJobContext(JobContext jobContext, JobContext databaseBootstrapJobContext) {
        String key = ReplicationJobMetrics.EXPORT_TOTAL.getName();
        String value = getFromJobContext(databaseBootstrapJobContext, key);
        jobProgress.setExportTotal(Long.parseLong(value));
        jobContext.getJobContextMap().put(key, value);

        key = ReplicationJobMetrics.EXPORT_COMPLETED.getName();
        value = getFromJobContext(databaseBootstrapJobContext, key);
        jobProgress.setExportCompleted(Long.parseLong(value));
        jobContext.getJobContextMap().put(key, value);

        key = ReplicationJobMetrics.IMPORT_TOTAL.getName();
        value = getFromJobContext(databaseBootstrapJobContext, key);
        jobProgress.setImportTotal(Long.parseLong(value));
        jobContext.getJobContextMap().put(key, value);

        key = ReplicationJobMetrics.IMPORT_COMPLETED.getName();
        value = getFromJobContext(databaseBootstrapJobContext, key);
        jobProgress.setImportCompleted(Long.parseLong(value));
        jobContext.getJobContextMap().put(key, value);

        jobProgress.setTotal(jobProgress.getImportTotal());
        jobProgress.setCompleted(jobProgress.getImportCompleted());
    }

    private String getFromJobContext(JobContext jobContext, String key) {
        return jobContext.getJobContextMap().get(key);
    }

}
