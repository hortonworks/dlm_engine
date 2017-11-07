/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.metrics.HiveReplicationMetrics;
import com.hortonworks.beacon.metrics.JobMetrics;
import com.hortonworks.beacon.metrics.JobMetricsHandler;
import com.hortonworks.beacon.metrics.ProgressUnit;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import com.hortonworks.beacon.metrics.util.ReplicationMetricsUtils;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.util.HiveActionType;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.jdbc.HiveStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hortonworks.beacon.replication.ReplicationUtils.getInstanceTrackingInfo;

/**
 * Abstract class for Replication.
 */
public abstract class InstanceReplication {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceReplication.class);

    protected static final String DUMP_DIRECTORY = "dumpDirectory";
    public static final String INSTANCE_EXECUTION_STATUS = "instanceExecutionStatus";

    private ReplicationJobDetails details;
    protected Properties properties;
    private InstanceExecutionDetails instanceExecutionDetails;


    public InstanceReplication(ReplicationJobDetails details) {
        this.details = details;
        this.properties = details.getProperties();
        this.instanceExecutionDetails = new InstanceExecutionDetails();
    }

    public Properties getProperties() {
        return properties;
    }

    public ReplicationJobDetails getDetails() {
        return details;
    }

    private InstanceExecutionDetails getInstanceExecutionDetails() {
        return instanceExecutionDetails;
    }

    protected void setInstanceExecutionDetails(JobContext jobContext, JobStatus jobStatus) throws BeaconException {
        setInstanceExecutionDetails(jobContext, jobStatus, jobStatus.name());
    }

    protected void setInstanceExecutionDetails(JobContext jobContext, JobStatus jobStatus,
                                               String message) throws BeaconException {
        setInstanceExecutionDetails(jobContext, jobStatus, message, null);
    }

    protected void setInstanceExecutionDetails(JobContext jobContext, JobStatus jobStatus,
                                               String message, Job job) throws BeaconException {
        getInstanceExecutionDetails().updateJobExecutionDetails(jobStatus.name(), message, getJob(job));
        jobContext.getJobContextMap().put(INSTANCE_EXECUTION_STATUS,
                getInstanceExecutionDetails().toJsonString());
    }

    protected String getJob(Job job) {
        return ((job != null) && (job.getJobID() != null)) ? job.getJobID().toString() : null;
    }

    private String getTrackingInfoAsJsonString(String jobId, String instanceId,
                                               ReplicationMetrics.JobType jobType,
                                               Map<String, Long> metrics,
                                               ProgressUnit progressUnit) throws BeaconException {
        ReplicationMetrics replicationMetrics = new ReplicationMetrics();
        if (metrics != null) {
            replicationMetrics.updateReplicationMetricsDetails(jobId, jobType, metrics, progressUnit);
        } else {
            replicationMetrics.updateReplicationMetricsDetails(jobId, jobType,
                    new HashMap<String, Long>(), progressUnit);
        }
        return getTrackingInfoAsJsonString(instanceId, jobType, replicationMetrics);
    }

    private String getTrackingInfoAsJsonString(Map<String, Long> metrics,
                                                ProgressUnit progressUnit) throws BeaconException {
        ReplicationMetrics replicationMetrics = new ReplicationMetrics();
        if (metrics != null) {
            replicationMetrics.updateReplicationMetricsDetails(metrics, progressUnit);
        } else {
            replicationMetrics.updateReplicationMetricsDetails(new HashMap<String, Long>(), progressUnit);
        }
        return replicationMetrics.toJsonString();
    }

    private static String getTrackingInfoAsJsonString(String instanceId,
                                                      ReplicationMetrics.JobType jobType,
                                                      ReplicationMetrics replicationMetrics) throws BeaconException {
        String trackingInfo = getInstanceTrackingInfo(instanceId);

        if (StringUtils.isNotBlank(trackingInfo)) {
            List<ReplicationMetrics> metrics = ReplicationMetricsUtils.getListOfReplicationMetrics(trackingInfo);
            if (metrics == null || metrics.isEmpty()) {
                throw new BeaconException(MessageCode.COMM_010008.name(), "metrics");
            } else {
                switch (jobType) {
                    case MAIN:
                        // Only main job. Just update it.
                        return replicationMetrics.toJsonString();

                    case RECOVERY:
                        // Check if there is already an recovery job
                        if (metrics.size() > 1) {
                            metrics.remove(1);
                            metrics.add(replicationMetrics);
                        } else {
                            // Currently no recovery job in tracking info. Add it
                            metrics.add(replicationMetrics);
                        }
                        return ReplicationMetricsUtils.toJsonString(metrics);

                    default:
                        LOG.error("Current job type is not MAIN or RECOVERY");
                        break;
                }
            }
        }
        return replicationMetrics.toJsonString();
    }

    private void updateTrackingInfo(JobContext jobContext, String jsonString) throws BeaconException {
        try {
            ReplicationUtils.storeTrackingInfo(jobContext, jsonString);
        } catch (BeaconException e) {
            LOG.error("Exception occurred while storing replication metrics info: {}", e.getMessage());
            throw new BeaconException(e);
        }
    }

    protected void captureFSReplicationMetrics(Job job, ReplicationMetrics.JobType jobType,
                                               JobContext jobContext, ReplicationType replicationType,
                                               boolean isJobComplete) throws IOException, BeaconException {
        JobMetrics fsReplicationMetrics = JobMetricsHandler.getMetricsType(replicationType);
        if (fsReplicationMetrics != null) {
            fsReplicationMetrics.obtainJobMetrics(job, isJobComplete);
            Map<String, Long> metrics = fsReplicationMetrics.getMetricsMap();
            String replicationMetricsJsonString = getTrackingInfoAsJsonString(getJob(job),
                    jobContext.getJobInstanceId(), jobType, metrics, ProgressUnit.MAPTASKS);
            updateTrackingInfo(jobContext, replicationMetricsJsonString);
        }
    }

    protected void getFSReplicationProgress(ScheduledThreadPoolExecutor timer, final JobContext jobContext,
                                            final Job job, final ReplicationMetrics.JobType jobType,
                                            int replicationMetricsInterval)
            throws IOException, BeaconException {
        timer.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    captureFSReplicationMetrics(job, jobType, jobContext, ReplicationType.FS, false);
                } catch (IOException | BeaconException e) {
                    LOG.error("Exception occurred while populating metrics periodically: {}", e.getMessage());
                }
            }
        }, 0, replicationMetricsInterval, TimeUnit.SECONDS);
    }

    private void captureHiveReplicationMetrics(JobContext jobContext, HiveActionType actionType, boolean bootstrap,
                                               List<String> queryLog) throws BeaconException {
        try {
            JobMetrics hiveReplicationMetrics = JobMetricsHandler.getMetricsType(ReplicationType.HIVE);
            if (hiveReplicationMetrics!=null && queryLog.size()!=0) {
                ((HiveReplicationMetrics) hiveReplicationMetrics).obtainJobMetrics(jobContext, queryLog, actionType);
                Map<String, Long> metrics = ((HiveReplicationMetrics)hiveReplicationMetrics).getMetricsMap();
                String replicationMetricsJsonString = getTrackingInfoAsJsonString(metrics,
                        (bootstrap ? ProgressUnit.TABLE : ProgressUnit.EVENTS));
                updateTrackingInfo(jobContext, replicationMetricsJsonString);
            }
        } catch (Exception e) {
            LOG.error("Error getting hive replication messages: {}", e.getMessage());
            throw new BeaconException(e);
        }
    }

    protected void getHiveReplicationProgress(ScheduledThreadPoolExecutor timer, final JobContext jobContext,
                                              final HiveActionType hiveActionType,
                                              int replicationMetricsInterval,
                                              final Statement statement) throws BeaconException {
        timer.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    HiveStatement hiveStatement = (HiveStatement) statement;
                    List<String> querylog = hiveStatement.getQueryLog();
                    boolean bootstrap = false;
                    if (jobContext.getJobContextMap().get(HiveDRUtils.BOOTSTRAP) != null) {
                        bootstrap = Boolean.parseBoolean(jobContext.getJobContextMap().get(HiveDRUtils.BOOTSTRAP));
                    }
                    captureHiveReplicationMetrics(jobContext, hiveActionType, bootstrap, querylog);
                } catch (SQLException | BeaconException e) {
                    LOG.error("Exception occurred while obtaining Hive metrics periodically:", e.getMessage());
                }
            }
        }, 0, replicationMetricsInterval, TimeUnit.MILLISECONDS);
    }

    protected void initializeProperties() throws BeaconException {
        String sourceCN = properties.getProperty(HiveDRProperties.SOURCE_CLUSTER_NAME.getName());
        String targetCN = properties.getProperty(HiveDRProperties.TARGET_CLUSTER_NAME.getName());
        Cluster sourceCluster = ClusterHelper.getActiveCluster(sourceCN);
        Cluster targetCluster = ClusterHelper.getActiveCluster(targetCN);
        properties.setProperty(HiveDRProperties.SOURCE_HS2_URI.getName(), sourceCluster.getHsEndpoint());
        properties.setProperty(HiveDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
        properties.setProperty(HiveDRProperties.TARGET_HS2_URI.getName(), targetCluster.getHsEndpoint());
        properties.setProperty(HiveDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());

        if (ClusterHelper.isHighlyAvailableHDFS(sourceCluster.getCustomProperties())) {
            Map<String, String> haConfigs = getHAConfigs(sourceCluster.getCustomProperties(),
                    targetCluster.getCustomProperties());
            for (Map.Entry<String, String> haConfig : haConfigs.entrySet()) {
                properties.setProperty(haConfig.getKey(), haConfig.getValue());
            }
        }
    }

    public static Map<String, String> getHAConfigs(Properties sourceProperties, Properties targetProperties) {
        Map<String, String> haConfigsMap = new HashMap<>();
        List<String> haConfigKeyList = new ArrayList<>();
        for(Map.Entry<Object, Object> property: sourceProperties.entrySet()) {
            if (property.getKey().toString().startsWith("dfs.")) {
                haConfigsMap.put(property.getKey().toString(), property.getValue().toString());
                haConfigKeyList.add(property.getKey().toString());
            }
        }
        for(Map.Entry<Object, Object> property: targetProperties.entrySet()) {
            if (property.getKey().toString().startsWith("dfs.")) {
                haConfigsMap.put(property.getKey().toString(), property.getValue().toString());
                haConfigKeyList.add(property.getKey().toString());
            }
        }
        String sourceHaNameservices = sourceProperties.getProperty(BeaconConstants.DFS_NAMESERVICES);
        String targetHaNameservices = targetProperties.getProperty(BeaconConstants.DFS_NAMESERVICES);
        haConfigsMap.put(BeaconConstants.DFS_NAMESERVICES,
                sourceHaNameservices + BeaconConstants.COMMA_SEPARATOR + targetHaNameservices);
        haConfigKeyList.add(BeaconConstants.DFS_NAMESERVICES);
        haConfigsMap.put(BeaconConstants.DFS_INTERNAL_NAMESERVICES, targetHaNameservices);
        haConfigKeyList.add(BeaconConstants.DFS_INTERNAL_NAMESERVICES);
        String haFailOverKey = BeaconConstants.DFS_CLIENT_FAILOVER_PROXY_PROVIDER + BeaconConstants.DOT_SEPARATOR
                + sourceHaNameservices;
        haConfigsMap.put(haFailOverKey, sourceProperties.getProperty(haFailOverKey));
        haConfigKeyList.add(haFailOverKey);
        LOG.info("Hadoop Configuration for Distcp: [{}]", haConfigsMap.toString());
        String haConfigKeys = StringUtils.join(haConfigKeyList, BeaconConstants.COMMA_SEPARATOR);
        haConfigsMap.put(BeaconConstants.HA_CONFIG_KEYS, haConfigKeys);
        return haConfigsMap;
    }
}
