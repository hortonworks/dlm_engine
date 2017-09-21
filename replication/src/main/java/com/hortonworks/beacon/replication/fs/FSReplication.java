/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication.fs;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import com.hortonworks.beacon.metrics.util.ReplicationMetricsUtils;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DefaultFilter;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_FILTERS_CLASS;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_LISTSTATUS_THREADS;

/**
 * FileSystem Replication implementation.
 */
public class FSReplication extends InstanceReplication implements BeaconJob {

    private static final BeaconLog LOG = BeaconLog.getLog(FSReplication.class);

    private String sourceStagingUri;
    private String targetStagingUri;
    private FileSystem sourceFs;
    private FileSystem targetFs;
    private boolean isSnapshot;
    private boolean isHCFS;
    private static final int MAX_JOB_RETRIES = 10;
    private static final String FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";

    public FSReplication(ReplicationJobDetails details) {
        super(details);
        isSnapshot = false;
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        try {
            BeaconLogUtils.setLogInfo(jobContext.getJobInstanceId());
            initializeProperties();
            initializeFileSystem();
            String sourceDataset = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
            String targetDataset = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());
            sourceStagingUri = FSUtils.getStagingUri(properties.getProperty(FSDRProperties.SOURCE_NN.getName()),
                    sourceDataset);
            targetStagingUri = FSUtils.getStagingUri(properties.getProperty(FSDRProperties.TARGET_NN.getName()),
                    targetDataset);
            isSnapshot = FSSnapshotUtils.isDirectorySnapshottable(sourceFs, targetFs,
                    sourceStagingUri, targetStagingUri);
            if (FSUtils.isHCFS(new Path(sourceStagingUri)) || FSUtils.isHCFS(new Path(targetStagingUri))) {
                isHCFS = true;
            }
        } catch (Exception e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), null);
            cleanUp(jobContext);
            throw new BeaconException(MessageCode.REPL_000004.name(), e, e.getMessage());
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        Job job = null;
        String fsReplicationName;
        try {
            fsReplicationName = getFSReplicationName();
            job = performCopy(jobContext, fsReplicationName, ReplicationMetrics.JobType.MAIN);
            if (job == null) {
                throw new BeaconException(MessageCode.COMM_010008.name(), "FS Replication job");
            }
        } catch (InterruptedException e) {
            cleanUp(jobContext);
            throw new BeaconException(e);
        } catch (Exception e) {
            LOG.error(MessageCode.REPL_000032.name(), e.getMessage());
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), job);
            cleanUp(jobContext);
            throw new BeaconException(e);
        }
        performPostReplJobExecution(jobContext, job, fsReplicationName,
                ReplicationMetrics.JobType.MAIN);
    }

    Job performCopy(JobContext jobContext,
                    String toSnapshot,
                    ReplicationMetrics.JobType jobType) throws BeaconException, InterruptedException {
        return performCopy(jobContext, toSnapshot,
                getLatestSnapshotOnTargetAvailableOnSource(), jobType);
    }

    Job performCopy(JobContext jobContext, String toSnapshot, String fromSnapshot,
                    ReplicationMetrics.JobType jobType) throws BeaconException, InterruptedException {
        Job job = null;
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
        try {
            boolean isInRecoveryMode = jobType == ReplicationMetrics.JobType.RECOVERY;
            DistCpOptions options = getDistCpOptions(toSnapshot, fromSnapshot, isInRecoveryMode);
            LOG.info(MessageCode.REPL_000033.name(), sourceStagingUri, targetStagingUri);
            Configuration conf = getConfiguration();
            DistCp distCp = new DistCp(conf, options);
            job = distCp.createAndSubmitJob();
            LOG.info(MessageCode.REPL_000034.name(), getJob(job), jobContext.getJobInstanceId());
            handlePostSubmit(timer, jobContext, job, jobType, distCp);

        } catch (InterruptedException e) {
            checkJobInterruption(jobContext, job);
            throw e;
        } catch (Exception e) {
            LOG.error(MessageCode.REPL_000035.name(), e.getMessage());
            throw new BeaconException(e);
        } finally {
            timer.shutdown();
        }
        return job;
    }

    private Configuration getConfiguration() {
        Configuration conf = getHAConfigOrDefault();
        conf.set(BeaconConstants.MAPRED_QUEUE_NAME, properties.getProperty(FSDRProperties.QUEUE_NAME.getName()));
        conf.set(CONF_LABEL_FILTERS_CLASS, DefaultFilter.class.getName());
        conf.setInt(CONF_LABEL_LISTSTATUS_THREADS, 20);
        return conf;
    }

    private Configuration getHAConfigOrDefault() {
        Configuration conf = new Configuration();
        if (properties.containsKey(BeaconConstants.HA_CONFIG_KEYS)) {
            String haConfigKeys = properties.getProperty(BeaconConstants.HA_CONFIG_KEYS);
            for(String haConfigKey: haConfigKeys.split(BeaconConstants.COMMA_SEPARATOR)) {
                conf.set(haConfigKey, properties.getProperty(haConfigKey));
            }
        }
        return conf;
    }

    private void handlePostSubmit(ScheduledThreadPoolExecutor timer, JobContext jobContext,
                                  final Job job, ReplicationMetrics.JobType jobType, DistCp distCp) throws Exception {
        getFSReplicationProgress(timer, jobContext, job, jobType,
                ReplicationUtils.getReplicationMetricsInterval());
        distCp.waitForJobCompletion(job);
    }

    private static ReplicationMetrics getCurrentJobDetails(JobContext jobContext) throws BeaconException {
        String instanceId = jobContext.getJobInstanceId();
        String trackingInfo = ReplicationUtils.getInstanceTrackingInfo(instanceId);

        List<ReplicationMetrics> metrics = ReplicationMetricsUtils.getListOfReplicationMetrics(trackingInfo);
        if (metrics == null || metrics.isEmpty()) {
            LOG.info(MessageCode.REPL_000036.name());
            return null;
        }

        // List can have only 2 jobs: one main job and one recovery distcp job
        if (metrics.size() > 1) {
            // Recovery has kicked in, return recovery job id
            return metrics.get(1);
        } else {
            return metrics.get(0);
        }
    }

    private String getLatestSnapshotOnTargetAvailableOnSource() throws BeaconException {
        String fromSnapshot = null;

        try {
            LOG.info(MessageCode.REPL_000037.name());
            if (isSnapshot && targetFs.exists(new Path(targetStagingUri))) {
                fromSnapshot = FSSnapshotUtils.findLatestReplicatedSnapshot((DistributedFileSystem) sourceFs,
                        (DistributedFileSystem) targetFs, sourceStagingUri, targetStagingUri);
            }
        } catch (IOException e) {
            LOG.error(MessageCode.REPL_000005.name(), targetStagingUri);
            throw new BeaconException(MessageCode.REPL_000005.name(), targetStagingUri);
        }
        return fromSnapshot;
    }

    protected void initializeProperties() throws BeaconException {
        String sourceDS = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
        String targetDS = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());
        String sourceCN = properties.getProperty(FSDRProperties.SOURCE_CLUSTER_NAME.getName());
        String targetCN = properties.getProperty(FSDRProperties.TARGET_CLUSTER_NAME.getName());

        if (!FSUtils.isHCFS(new Path(sourceDS))) {
            Cluster sourceCluster = ClusterHelper.getActiveCluster(sourceCN);
            properties.setProperty(FSDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
        } else {
            properties.setProperty(FSDRProperties.SOURCE_NN.getName(), sourceDS);
        }

        if (!FSUtils.isHCFS(new Path(targetDS))) {
            Cluster targetCluster = ClusterHelper.getActiveCluster(targetCN);
            properties.setProperty(FSDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());
        } else {
            properties.setProperty(FSDRProperties.TARGET_NN.getName(), targetDS);
        }

        Cluster sourceCluster = ClusterHelper.getActiveCluster(sourceCN);
        if (ClusterHelper.isHighlyAvailableHDFS(sourceCluster.getCustomProperties())) {
            Cluster targetCluster = ClusterHelper.getActiveCluster(targetCN);
            Map<String, String> haConfigs = FSPolicyHelper.getHAConfigs(sourceCluster.getCustomProperties(),
                    targetCluster.getCustomProperties());
            for (Map.Entry<String, String> haConfig : haConfigs.entrySet()) {
                properties.setProperty(haConfig.getKey(), haConfig.getValue());
            }
        }
    }

    private void initializeFileSystem() throws BeaconException {
        try {
            String sourceClusterName = properties.getProperty(FSDRProperties.SOURCE_CLUSTER_NAME.getName());
            String targetClusterName = properties.getProperty(FSDRProperties.TARGET_CLUSTER_NAME.getName());
            Configuration sourceConf = ClusterHelper.getHAConfigurationOrDefault(sourceClusterName);
            Configuration targetConf = ClusterHelper.getHAConfigurationOrDefault(targetClusterName);
            sourceConf.setBoolean(FS_HDFS_IMPL_DISABLE_CACHE, true);
            targetConf.setBoolean(FS_HDFS_IMPL_DISABLE_CACHE, true);
            sourceFs = FSUtils.getFileSystem(properties.getProperty(
                    FSDRProperties.SOURCE_NN.getName()), sourceConf, isHCFS);
            targetFs = FSUtils.getFileSystem(properties.getProperty(
                    FSDRProperties.TARGET_NN.getName()), targetConf, isHCFS);
        } catch (BeaconException e) {
            LOG.error(MessageCode.REPL_000038.name(), e);
            throw new BeaconException(e.getMessage());
        }
    }

    private String getFSReplicationName() throws BeaconException {
        boolean tdeEncryptionEnabled = Boolean.parseBoolean(properties.getProperty(
                FSDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
        LOG.info(MessageCode.REPL_000039.name(), tdeEncryptionEnabled);
        // check if source and target path's exist and are snapshot-able
        String fsReplicationName = properties.getProperty(FSDRProperties.JOB_NAME.getName())
                + "-" + System.currentTimeMillis();
        if (!tdeEncryptionEnabled) {
            if (properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()) != null) {
                try {
                    if (isSnapshot) {
                        fsReplicationName = FSSnapshotUtils.SNAPSHOT_PREFIX
                                + properties.getProperty(FSDRProperties.JOB_NAME.getName())
                                + "-" + System.currentTimeMillis();
                        FSSnapshotUtils.handleSnapshotCreation(sourceFs, sourceStagingUri, fsReplicationName);
                    }
                } catch (BeaconException e) {
                    throw new BeaconException(e);
                }
            }
        }
        return fsReplicationName;
    }

    private DistCpOptions getDistCpOptions(String toSnapshot, String fromSnapshot, boolean isInRecoveryMode)
            throws BeaconException, IOException {
        // DistCpOptions expects the first argument to be a file OR a list of Paths

        List<Path> sourceUris = new ArrayList<>();
        if (isInRecoveryMode) {
            sourceUris.add(new Path(targetStagingUri));
        } else {
            sourceUris.add(new Path(sourceStagingUri));
        }

        return DistCpOptionsUtil.getDistCpOptions(properties, sourceUris, new Path(targetStagingUri),
                isSnapshot, fromSnapshot, toSnapshot, isInRecoveryMode);
    }

    private void performPostReplJobExecution(JobContext jobContext, Job job, String fsReplicationName,
                                             ReplicationMetrics.JobType jobType) throws BeaconException {
        try {
            if (job.isComplete() && job.isSuccessful()) {
                if (isSnapshot) {
                    try {
                        FSSnapshotUtils.handleSnapshotCreation(targetFs, targetStagingUri, fsReplicationName);
                        FSSnapshotUtils.handleSnapshotEviction(sourceFs, properties, sourceStagingUri);
                        FSSnapshotUtils.handleSnapshotEviction(targetFs, properties, targetStagingUri);
                    } catch (BeaconException e) {
                        throw new BeaconException(MessageCode.REPL_000006.name(), e);
                    }
                }
                LOG.info(MessageCode.REPL_000077.name());
                captureFSReplicationMetrics(job, jobType, jobContext, ReplicationType.FS, true);
                setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS, JobStatus.SUCCESS.name(), job);
            } else {
                throw new BeaconException(MessageCode.REPL_000007.name(), getJob(job));
            }
        } catch (Exception e) {
            LOG.error(MessageCode.REPL_000032.name(), e.getMessage());
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), job);
            cleanUp(jobContext);
            throw new BeaconException(e);
        }
    }

    private void checkJobInterruption(JobContext jobContext, Job job) throws BeaconException {
        if (job != null) {
            try {
                LOG.error(MessageCode.REPL_000040.name(), getJob(job));
                job.killJob();
                setInstanceExecutionDetails(jobContext, JobStatus.KILLED, "job killed", job);
            } catch (IOException ioe) {
                LOG.error(ioe.getMessage(), ioe);
            }
        }
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {
        try {
            if (sourceFs != null) {
                sourceFs.close();
            }
            if (targetFs != null) {
                targetFs.close();
            }
        } catch (Exception e) {
            throw new BeaconException(MessageCode.REPL_000008.name(), e);
        }
    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        LOG.info(MessageCode.COMM_010012.name(), jobContext.getJobInstanceId());

        ReplicationMetrics currentJobMetric = getCurrentJobDetails(jobContext);
        if (currentJobMetric == null) {
            //Case, when previous instance was failed/killed.
            jobContext.setPerformJobAfterRecovery(true);
            if (!isSnapshot) {
                LOG.info(MessageCode.REPL_000041.name(), jobContext.getJobInstanceId());
                return;
            }
            handleRecovery(jobContext);
            return;
        }

        LOG.info(MessageCode.REPL_000044.name(), currentJobMetric.getJobId(), currentJobMetric.getJobType());

        RunningJob job = getJobWithRetries(currentJobMetric.getJobId());
        if (job != null) {
            Job currentJob;
            org.apache.hadoop.mapred.JobStatus jobStatus;
            try {
                jobStatus = job.getJobStatus();
                currentJob = getJobClient().getClusterHandle().getJob(job.getID());
            } catch (IOException | InterruptedException e) {
                throw new BeaconException(e);
            }

            if (org.apache.hadoop.mapred.JobStatus.State.RUNNING.getValue() == jobStatus.getRunState()
                    || org.apache.hadoop.mapred.JobStatus.State.PREP.getValue() == jobStatus.getRunState()) {
                ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
                DistCp distCp;
                try {
                    distCp = new DistCp(getConfiguration(), getDistCpOptions(null, null, false));
                    handlePostSubmit(timer, jobContext, currentJob, ReplicationMetrics.JobType.MAIN, distCp);
                    performPostReplJobExecution(jobContext, currentJob, getFSReplicationName(),
                            ReplicationMetrics.JobType.MAIN);
                    jobContext.setPerformJobAfterRecovery(false);
                } catch (Exception e) {
                    throw new BeaconException(e);
                } finally {
                    timer.shutdown();
                }
            } else if (org.apache.hadoop.mapred.JobStatus.State.SUCCEEDED.getValue() == jobStatus.getRunState()) {
                performPostReplJobExecution(jobContext, currentJob,
                        getFSReplicationName(), ReplicationMetrics.JobType.MAIN);
                jobContext.setPerformJobAfterRecovery(false);
            } else {
                jobContext.setPerformJobAfterRecovery(true);
                if (!isSnapshot) {
                    LOG.info(MessageCode.REPL_000041.name(), jobContext.getJobInstanceId());
                    return;
                }
                // Job failed for snapshot based replication. Try recovering.
                handleRecovery(jobContext);
            }
        } else {
            if (!isSnapshot) {
                LOG.info(MessageCode.REPL_000041.name(), jobContext.getJobInstanceId());
                jobContext.setPerformJobAfterRecovery(true);
                return;
            }
            LOG.error(MessageCode.REPL_000078.name(), currentJobMetric.getJobId());
            throw new BeaconException(MessageCode.REPL_000078.name(), currentJobMetric.getJobId());
        }
    }

    private void handleRecovery(JobContext jobContext) throws BeaconException {
        String fromSnapshot = getLatestSnapshotOnTargetAvailableOnSource();
        if (StringUtils.isBlank(fromSnapshot)) {
            LOG.info(MessageCode.REPL_000045.name(), jobContext.getJobInstanceId());
            return;
        }
        // Create current state on the target cluster
        String toSnapshot = "tempRecoverySnapshot";
        FSSnapshotUtils.checkAndCreateSnapshot(targetFs, targetStagingUri, toSnapshot);

        // toSnapshot is created for recovery so swap the parameter between (toSnapshot and fromSnapshot)
        Job job = null;
        try {
            SnapshotDiffReport diffReport = ((DistributedFileSystem) targetFs).getSnapshotDiffReport(
                    new Path(targetStagingUri), fromSnapshot, toSnapshot);
            List diffList = diffReport.getDiffList();
            if (diffList == null || diffList.isEmpty()) {
                LOG.info(MessageCode.REPL_000046.name(), jobContext.getJobInstanceId());
                FSSnapshotUtils.checkAndDeleteSnapshot(targetFs, targetStagingUri, toSnapshot);
                return;
            }
            LOG.info(MessageCode.REPL_000047.name(), jobContext.getJobInstanceId());
            try {
                job = performCopy(jobContext, fromSnapshot, toSnapshot, ReplicationMetrics.JobType.RECOVERY);
                FSSnapshotUtils.checkAndDeleteSnapshot(targetFs, targetStagingUri, toSnapshot);
                // Re-create the same snapshot for replication purpose.
                FSSnapshotUtils.checkAndCreateSnapshot(targetFs, targetStagingUri, fromSnapshot);
            } catch (InterruptedException e) {
                cleanUp(jobContext);
                throw new BeaconException(e);
            }
            if (job == null) {
                throw new BeaconException(MessageCode.COMM_010008.name(), "FS Replication recovery job");
            }
        } catch (Exception e) {
            LOG.error(MessageCode.REPL_000009.name(), e, targetStagingUri, fromSnapshot, toSnapshot);
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), job);
            throw new BeaconException(MessageCode.REPL_000009.name(), e, targetStagingUri, fromSnapshot, toSnapshot);
        }
    }

    private RunningJob getJobWithRetries(String jobId) throws BeaconException {
        RunningJob runningJob = null;
        if (jobId != null) {
            int hadoopJobLookupRetries = BeaconConfig.getInstance().getEngine().getHadoopJobLookupRetries();
            int hadoopJobLookupDelay = BeaconConfig.getInstance().getEngine().getHadoopJobLookupDelay();
            int retries = 0;
            int maxJobRetries = hadoopJobLookupRetries > MAX_JOB_RETRIES ? MAX_JOB_RETRIES : hadoopJobLookupRetries;
            while (retries++ < maxJobRetries) {
                LOG.info(MessageCode.REPL_000048.name(), jobId, retries);
                try {
                    runningJob = getJobClient().getJob(JobID.forName(jobId));
                } catch (IOException ioe) {
                    throw new BeaconException(ioe);
                }

                if (runningJob != null) {
                    break;
                }

                try {
                    Thread.sleep(hadoopJobLookupDelay * 1000);
                } catch (InterruptedException e) {
                    // Do nothing
                }
            }
        }
        return runningJob;
    }

    private JobClient getJobClient() throws BeaconException {
        try {
            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
            JobClient jobClient = loginUser.doAs(new PrivilegedExceptionAction<JobClient>() {
                public JobClient run() throws Exception {
                    return new JobClient(new JobConf());
                }
            });
            return jobClient;
        } catch (InterruptedException | IOException e) {
            throw new BeaconException(MessageCode.REPL_000010.name(), e, e.getMessage());
        }
    }

}
