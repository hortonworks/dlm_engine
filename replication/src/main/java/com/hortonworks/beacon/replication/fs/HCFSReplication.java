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

package com.hortonworks.beacon.replication.fs;

import static com.hortonworks.beacon.util.FSUtils.merge;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_FILTERS_CLASS;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_LISTSTATUS_THREADS;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.entity.util.CloudCredDao;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DefaultFilter;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.util.FSUtils;

/**
 * HCFS FileSystem Replication implementation.
 */
public class HCFSReplication extends FSReplication implements BeaconJob {

    private static final Logger LOG = LoggerFactory.getLogger(HCFSReplication.class);

    private String sourceStagingUri;
    private String targetStagingUri;
    private boolean isSnapshot;
    private boolean isPushRepl;

    public HCFSReplication(ReplicationJobDetails details) {
        super(details);
        isSnapshot = false;
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        try {
            BeaconLogUtils.prefixId(jobContext.getJobInstanceId());
            initializeProperties();
            initializeFileSystem();
            String sourceDataset = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
            String targetDataset = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());
            sourceStagingUri = FSUtils.getStagingUri(properties.getProperty(FSDRProperties.SOURCE_DATASET.getName()),
                    sourceDataset);
            targetStagingUri  = FSUtils.getStagingUri(properties.getProperty(FSDRProperties.TARGET_DATASET.getName()),
                    targetDataset);
        } catch (Exception e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), null);
            cleanUp(jobContext);
            throw new BeaconException("Exception occurred in HCFS init: ", e);
        } finally{
            BeaconLogUtils.deletePrefix();
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        Job job = null;
        try {
            job = performCopy(jobContext, ReplicationMetrics.JobType.MAIN);
            performPostReplJobExecution(jobContext, job, ReplicationMetrics.JobType.MAIN);
        } catch (InterruptedException e) {
            cleanUp(jobContext);
            throw new BeaconException(e);
        } catch (Exception e) {
            LOG.error("Exception occurred in HCFS replication: {}", e.getMessage());
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), job);
            cleanUp(jobContext);
            throw new BeaconException(e);
        }
    }

    private Job performCopy(JobContext jobContext, ReplicationMetrics.JobType jobType)
            throws BeaconException, InterruptedException {
        Job job = null;
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
        String fromSnapshot = null;
        String toSnapshot = null;
        try {
            if (isSnapshot && isPushRepl) {
                toSnapshot = FSSnapshotUtils.TEMP_REPLICATION_SNAPSHOT;
                String snapshotPrefix = FSSnapshotUtils.SNAPSHOT_PREFIX + getDetails().getName();
                fromSnapshot = FSSnapshotUtils.getLatestSnapshot(sourceFs, sourceStagingUri, snapshotPrefix);
                FSSnapshotUtils.checkAndCreateSnapshot(sourceFs, sourceStagingUri, toSnapshot);
            }
            DistCpOptions options = getDistCpOptions(fromSnapshot, toSnapshot);
            LOG.info("Started DistCp with source path: {} target path: {}", sourceStagingUri, targetStagingUri);
            Configuration conf = getHCFSConfiguration();
            DistCp distCp = new DistCp(conf, options);
            job = distCp.createAndSubmitJob();
            LOG.info("DistCp Hadoop job: {} for policy instance: [{}]", getJob(job), jobContext.getJobInstanceId());
            handlePostSubmit(timer, jobContext, job, jobType, distCp);
        } catch (InterruptedException e) {
            checkJobInterruption(jobContext, job);
            throw e;
        } catch (Exception e) {
            LOG.error("Exception occurred while performing copying of HCFS data: {}", e.getMessage());
            throw new BeaconException(e);
        } finally {
            timer.shutdown();
        }
        return job;
    }

    private Configuration getHCFSConfiguration() throws BeaconException, URISyntaxException {
        Configuration conf = getConfiguration();
        String cloudCredId = properties.getProperty(FSDRProperties.CLOUD_CRED.getName());
        BeaconCloudCred cloudCred = new BeaconCloudCred(new CloudCredDao().getCloudCred(cloudCredId));
        Configuration cloudConf = cloudCred.getHadoopConf(false);
        merge(cloudConf, cloudCred.getCloudEncryptionTypeConf(properties));
        merge(cloudConf, getConfigurationWithBucketEndPoint(cloudCred));
        return merge(conf, cloudConf);
    }

    private Configuration getConfigurationWithBucketEndPoint(BeaconCloudCred cloudCred) throws BeaconException,
            URISyntaxException {
        String cloudPath;
        String sourcePath = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
        if (PolicyHelper.isDatasetHCFS(sourcePath)) {
            cloudPath = sourcePath;
        } else {
            cloudPath = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());
        }
        return cloudCred.getBucketEndpointConf(cloudPath);
    }

    private Configuration getConfiguration() {
        Configuration conf = getHAConfigOrDefault();
        String queueName = properties.getProperty(FSDRProperties.QUEUE_NAME.getName());
        if (queueName != null) {
            conf.set(BeaconConstants.MAPRED_QUEUE_NAME, queueName);
        }

        conf.set(CONF_LABEL_FILTERS_CLASS, DefaultFilter.class.getName());
        conf.setInt(CONF_LABEL_LISTSTATUS_THREADS, 20);
        conf.set(DistCpConstants.DISTCP_EXCLUDE_FILE_REGEX, BeaconConfig.getInstance()
                .getEngine().getExcludeFileRegex());
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

    protected void initializeProperties() throws BeaconException {
        String sourceDS = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
        String targetDS = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());
        String sourceCN = properties.getProperty(FSDRProperties.SOURCE_CLUSTER_NAME.getName());
        String targetCN = properties.getProperty(FSDRProperties.TARGET_CLUSTER_NAME.getName());

        properties.setProperty(FSDRProperties.SOURCE_DATASET.getName(), sourceDS);
        properties.setProperty(FSDRProperties.TARGET_DATASET.getName(), targetDS);

        if (FSUtils.isHCFS(new Path(sourceDS))) {
            Cluster targetCluster = ClusterHelper.getActiveCluster(targetCN);
            properties.setProperty(FSDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());
            isPushRepl = false;
        } else {
            Cluster sourceCluster = ClusterHelper.getActiveCluster(sourceCN);
            properties.setProperty(FSDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
            isPushRepl = true;
            isSnapshot = SnapshotListing.get().isSnapshottable(sourceCN, sourceCluster.getFsEndpoint(), sourceDS);
        }
    }

    private void initializeFileSystem() throws BeaconException {
        try {
            if (isPushRepl) {
                String sourceClusterName = properties.getProperty(FSDRProperties.SOURCE_CLUSTER_NAME.getName());
                Configuration sourceConf = ClusterHelper.getHAConfigurationOrDefault(sourceClusterName);
                sourceConf.setBoolean(FS_HDFS_IMPL_DISABLE_CACHE, true);
                sourceFs = FSUtils.getFileSystem(properties.getProperty(
                        FSDRProperties.SOURCE_NN.getName()), sourceConf, true);
            } else {
                String targetClusterName = properties.getProperty(FSDRProperties.TARGET_CLUSTER_NAME.getName());
                Configuration targetConf = ClusterHelper.getHAConfigurationOrDefault(targetClusterName);
                targetConf.setBoolean(FS_HDFS_IMPL_DISABLE_CACHE, true);
                targetFs = FSUtils.getFileSystem(properties.getProperty(
                        FSDRProperties.TARGET_NN.getName()), targetConf, true);
            }
        } catch (BeaconException e) {
            LOG.error("Exception occurred while initializing DistributedFileSystem: {}", e);
            throw new BeaconException(e.getMessage());
        }
    }

    private DistCpOptions getDistCpOptions(String fromSnapshot, String toSnapshot) throws BeaconException, IOException {
        // DistCpOptions expects the first argument to be a file OR a list of Paths

        List<Path> sourceUris = new ArrayList<>();
        Path targetPath;
        sourceUris.add(new Path(sourceStagingUri));
        targetPath = new Path(targetStagingUri);

        return DistCpOptionsUtil.getHCFSDistCpOptions(properties, sourceUris, targetPath,
                isSnapshot, fromSnapshot, toSnapshot);
    }

    private void performPostReplJobExecution(JobContext jobContext, Job job,
                                             ReplicationMetrics.JobType jobType) throws BeaconException {
        try {
            if (job.isComplete() && job.isSuccessful()) {
                if (isSnapshot && isPushRepl) {
                    FSSnapshotUtils.checkAndRenameSnapshot(sourceFs, sourceStagingUri,
                            FSSnapshotUtils.TEMP_REPLICATION_SNAPSHOT,
                            FSSnapshotUtils.getSnapshotName(getDetails().getName()));
                    FSSnapshotUtils.handleSnapshotEviction(sourceFs, properties, sourceStagingUri);
                }
                LOG.info("HCFS Distcp copy is successful.");
                captureFSReplicationMetrics(job, jobType, jobContext, true);
                setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS, JobStatus.SUCCESS.name(), job);
            } else {
                throw new BeaconException("HCFS Job exception occurred: {}", getJob(job));
            }
        } catch (Exception e) {
            LOG.error("Exception occurred in HCFS replication: {}", e.getMessage());
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), job);
            cleanUp(jobContext);
            throw new BeaconException(e);
        }
    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        jobContext.setPerformJobAfterRecovery(true);
    }
}
