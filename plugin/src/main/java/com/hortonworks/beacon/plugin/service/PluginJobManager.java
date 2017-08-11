/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.plugin.service;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 *  Plugin Job manger.
 */
public class PluginJobManager extends InstanceReplication implements BeaconJob {
    private static final BeaconLog LOG = BeaconLog.getLog(PluginJobManager.class);
    private static final String PLUGIN_STAGING_PATH = "PLUGIN_STAGINGPATH";

    public PluginJobManager(ReplicationJobDetails details) {
        super(details);
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {

    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        Properties properties = getProperties();
        // get the plugin name
        String pluginName = properties.getProperty(PluginJobProperties.JOB_TYPE.getName());
        if (!PluginManagerService.isPluginRegistered(pluginName)) {
            throw new BeaconException(MessageCode.PLUG_000002.name(), pluginName);
        }

        Plugin plugin = PluginManagerService.getPlugin(pluginName);
        Plugin.Status pluginStatus = plugin.getStatus();
        // To-DO: Do we throw exception?
        if (Plugin.Status.ACTIVE != pluginStatus) {
            throw new BeaconException(MessageCode.PLUG_000003.name(), pluginName, pluginStatus);
        }

        String action = properties.getProperty(PluginJobProperties.JOBACTION_TYPE.getName());

        String dataset = properties.getProperty(PluginJobProperties.DATASET.getName());
        String datasetType = properties.getProperty(PluginJobProperties.DATASET_TYPE.getName());
        Cluster srcCluster = ClusterHelper.getActiveCluster(properties.getProperty(
                PluginJobProperties.SOURCE_CLUSTER.getName()));
        Cluster targetCluster = ClusterHelper.getActiveCluster(properties.getProperty(
                PluginJobProperties.TARGET_CLUSTER.getName()));
        DataSet pluginDataset = new DatasetImpl(dataset, DataSet.DataSetType.valueOf(datasetType.toUpperCase()),
                srcCluster, targetCluster);

        switch (PluginManagerService.getActionType(action)) {
            case EXPORT:
                Path path = plugin.exportData(pluginDataset);
                if (path == null) {
                    jobContext.getJobContextMap().put(PLUGIN_STAGING_PATH, null);
                } else {
                    jobContext.getJobContextMap().put(PLUGIN_STAGING_PATH, path.toString());
                }
                break;

            case IMPORT:
                String stagingPath = jobContext.getJobContextMap().get(PLUGIN_STAGING_PATH);
                if (StringUtils.isBlank(stagingPath)) {
                    LOG.info(MessageCode.PLUG_000008.name(), pluginDataset);
                    return;
                }

                plugin.importData(pluginDataset, new Path(stagingPath));
                break;

            default:
                throw new BeaconException(MessageCode.PLUG_000004.name(), action, pluginName);
        }
        setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS);
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {

    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        LOG.info(MessageCode.COMM_010012.name(), jobContext.getJobInstanceId());
    }
}
