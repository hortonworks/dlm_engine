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

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.Timer;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveServerClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.exceptions.BeaconSuspendException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.metrics.Progress;
import com.hortonworks.beacon.metrics.ProgressUnit;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.util.HiveActionType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.hortonworks.beacon.constants.BeaconConstants.HIVE_QUERY_ID;

/**
 * Import Hive Replication implementation.
 */
public class HiveImport extends InstanceReplication {

    private static final Logger LOG = LoggerFactory.getLogger(HiveImport.class);
    private String database;
    private Statement targetStatement = null;
    private ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);

    public HiveImport(ReplicationJobDetails details) {
        super(details);
        this.database = properties.getProperty(HiveDRProperties.TARGET_DATASET.getName());
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        try {
            initializeProperties();
            if (!Boolean.valueOf(Cluster.ClusterFields.CLOUDDATALAKE.getName())) {
                initializeFileSystem();
                initializeCustomProperties();
            }
        } catch (Exception e) {
            throw new BeaconException("Exception occurred initializing Hive Server: ", e);
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException, InterruptedException {
        final String methodName = this.getClass().getSimpleName() + '.'
                + Thread.currentThread().getStackTrace()[1].getMethodName();
        RequestContext requestContext = RequestContext.get();
        Timer methodTimer = requestContext.startTimer(methodName);
        try {
            String dumpDirectory = jobContext.getJobContextMap().get(DUMP_DIRECTORY);
            LOG.info("Location of repl dump directory: {}", dumpDirectory);
            if (StringUtils.isNotBlank(dumpDirectory)) {
                performImport(dumpDirectory, jobContext);
                LOG.info("Beacon Hive replication successful");
            } else {
                LOG.info("Repl Dump Directory is null, thus not performing Hive import");
                jobContext.getJobContextMap().put(BeaconConstants.END_TIME,
                        String.valueOf(System.currentTimeMillis()));
            }
        } finally {
            methodTimer.stop();
        }
        setBootstrapStatus(jobContext);
    }

    private void performImport(String dumpDirectory, JobContext jobContext)
            throws BeaconException, InterruptedException {
        LOG.info("Performing import for database: {}", database);
        ReplCommand replCommand = new ReplCommand(database);
        String replLoad = replCommand.getReplLoad(dumpDirectory);
        String configParams = HiveDRUtils.setConfigParameters(properties);
        if (StringUtils.isNotBlank(configParams)) {
            replLoad += " WITH (" + configParams + ")";
        }

        LOG.info("REPL Load statement: {}", replLoad);
        HiveServerClient hiveServerClient = null;
        try {
            if (jobContext.shouldInterrupt().get()) {
                throw new InterruptedException("before repl load");
            }
            String targetConnection = HiveDRUtils.getTargetConnectionString(properties);
            hiveServerClient = HiveClientFactory.getHiveServerClient(targetConnection);
            targetStatement = hiveServerClient.createStatement();
            getHiveReplicationProgress(timer, jobContext, HiveActionType.IMPORT,
                    ReplicationUtils.getReplicationMetricsInterval(), targetStatement);
            if (Boolean.valueOf(jobContext.getJobContextMap().get(HiveDRUtils.BOOTSTRAP))
                    && replCommand.getReplicatedEventId(targetStatement, properties) > 0) {
                jobContext.getJobContextMap().put(HiveDRUtils.BOOTSTRAP, "false");
                LOG.info("Bootstrap replication has already completed, skipping hive import.");
                return;
            }
            storeHiveQueryId(jobContext, properties.getProperty(HIVE_QUERY_ID));
            ((HiveStatement) targetStatement).executeAsync(replLoad);
            storeHiveQueryId(jobContext, targetStatement);
            targetStatement.getUpdateCount();
            LOG.info("REPL LOAD execution finished!");
        } catch (SQLException e) {
            if (e.getErrorCode() >= 20000 && e.getErrorCode() <= 29999) {
                throw new BeaconSuspendException(e, e.getErrorCode());
            }
            throw new BeaconException(e);
        } finally {
            LOG.debug("Capturing hive import metrics after job execution");
            jobContext.getJobContextMap().put(BeaconConstants.END_TIME,
                    String.valueOf(System.currentTimeMillis()));
            shutdownTimer();
            captureHiveReplicationMetrics(jobContext, HiveActionType.IMPORT, targetStatement);
            close(targetStatement);
            HiveClientFactory.close(hiveServerClient);
        }
    }

    @Override
    public void cleanUp(JobContext jobContext) {
    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        LOG.info("Recover policy instance: [{}]", jobContext.getJobInstanceId());
        ReplicationMetrics currentJobMetric = getCurrentJobDetails(jobContext);
        jobContext.setPerformJobAfterRecovery(true);
        if (currentJobMetric != null) {
            String queryId = currentJobMetric.getJobId();
            killHiveQuery(queryId);
        }
    }

    private void killHiveQuery(String queryId) throws BeaconException {
        if (StringUtils.isNotEmpty(queryId)) {
            LOG.info("Killing Hive query id: {}", queryId);
            HiveServerClient hiveServerClient = null;
            try {
                String targetConnection = HiveDRUtils.getTargetConnectionString(properties);
                hiveServerClient = HiveClientFactory.getHiveServerClient(targetConnection);
                hiveServerClient.killQuery(queryId, HiveDRUtils.getTargetHiveConf(properties));
            } finally {
                HiveClientFactory.close(hiveServerClient);
            }
        } else {
            LOG.debug("No Hive query id found!");
        }
    }

    @Override
    public void interrupt() throws BeaconException {
        shutdownTimer();
        try {
            if (targetStatement != null && !targetStatement.isClosed()) {
                LOG.debug("Interrupting Hive Import job!");
                targetStatement.cancel();
            }
        } catch (SQLException e) {
            throw new BeaconException("Unable to interrupt Hive Import job!", e);
        }
    }

    private void shutdownTimer() {
        if (!timer.isShutdown()) {
            timer.shutdownNow();
        }
    }

    private void storeHiveQueryId(final JobContext jobContext, final String queryId) {
        jobContext.setQueryId(queryId);
        boolean bootstrap = Boolean.valueOf(jobContext.getJobContextMap().get(HiveDRUtils.BOOTSTRAP));
        try {
            String details = getTrackingInfoAsJsonString(queryId, new Progress(),
                    ReplicationMetrics.JobType.MAIN, (bootstrap ? ProgressUnit.TABLE : ProgressUnit.EVENTS));
            ReplicationUtils.storeTrackingInfo(jobContext, details);
        } catch (BeaconException e) {
            LOG.error("Unable to persist query id: {}", queryId);
        }
    }

    private void storeHiveQueryId(final JobContext jobContext, final Statement statement) {
        try {
            String queryId = ((HiveStatement) statement).getQueryId();
            String previousQueryId = jobContext.getQueryId();
            LOG.info("Hive statement query id: {}, previous query id: {}", queryId, previousQueryId);
            if (StringUtils.isNotEmpty(queryId) && !previousQueryId.equals(queryId)) {
                LOG.info("Hive query id: {}", queryId);
                jobContext.setQueryId(queryId);
            } else {
                LOG.debug("Query execution finished before queryId retrieval or matches with the provided one.");
            }
        } catch (SQLException e) {
            LOG.error("Error while retrieving the query id.", e);
        }
    }

    private void setBootstrapStatus(JobContext jobContext) throws BeaconException {
        ReplCommand replCommand = new ReplCommand();
        HiveServerClient hiveServerClient = null;
        Statement statement = null;
        try {
            String targetConnection = HiveDRUtils.getTargetConnectionString(properties);
            hiveServerClient = HiveClientFactory.getHiveServerClient(targetConnection);
            statement = hiveServerClient.createStatement();
            if (Boolean.valueOf(jobContext.getJobContextMap().get(HiveDRUtils.BOOTSTRAP))
                    && replCommand.getReplicatedEventId(statement, properties) > 0) {
                jobContext.getJobContextMap().put(HiveDRUtils.BOOTSTRAP, "false");
            }
        } finally {
            close(statement);
            HiveClientFactory.close(hiveServerClient);
        }
    }
}
