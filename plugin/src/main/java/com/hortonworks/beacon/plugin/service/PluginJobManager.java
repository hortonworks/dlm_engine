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

package com.hortonworks.beacon.plugin.service;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 *  Plugin Job manger.
 */
public class PluginJobManager extends InstanceReplication {
    private static final Logger LOG = LoggerFactory.getLogger(PluginJobManager.class);
    private static final String PLUGIN_STAGING_PATH = "PLUGIN_STAGINGPATH";

    public PluginJobManager(ReplicationJobDetails details) {
        super(details);
    }

    @Override
    public void init(JobContext jobContext) {

    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        Properties properties = getProperties();
        // get the plugin name
        String pluginName = properties.getProperty(PluginJobProperties.JOB_TYPE.getName());
        if (!PluginManagerService.isPluginRegistered(pluginName)) {
            throw new BeaconException("Plugin {} is not registered. Cannot perform the job", pluginName);
        }

        Plugin plugin = PluginManagerService.getPlugin(pluginName);
        Plugin.Status pluginStatus = plugin.getStatus();
        // To-DO: Do we throw exception?
        if (Plugin.Status.ACTIVE != pluginStatus) {
            throw new BeaconException("Plugin {} is in {} and not in active state", pluginName, pluginStatus);
        }

        String action = properties.getProperty(PluginJobProperties.JOBACTION_TYPE.getName());

        String sourceDataset = properties.getProperty(PluginJobProperties.SOURCE_DATASET.getName());
        String targetDataset = properties.getProperty(PluginJobProperties.TARGET_DATASET.getName());
        String datasetType = properties.getProperty(PluginJobProperties.DATASET_TYPE.getName());
        Cluster srcCluster = null, targetCluster = null;
        if (!PolicyHelper.isDatasetHCFS(sourceDataset)) {
            srcCluster = ClusterHelper.getActiveCluster(properties.getProperty(
                    PluginJobProperties.SOURCE_CLUSTER.getName()));
        }
        if (!PolicyHelper.isDatasetHCFS(targetDataset)) {
            targetCluster = ClusterHelper.getActiveCluster(properties.getProperty(
                    PluginJobProperties.TARGET_CLUSTER.getName()));
        }
        String stagingDir = properties.getProperty(BeaconConstants.PLUGIN_STAGING_DIR);
        DataSet pluginDataset = new DatasetImpl(sourceDataset, targetDataset,
                DataSet.DataSetType.valueOf(datasetType.toUpperCase()), srcCluster, targetCluster, stagingDir);
        LOG.debug("Staging directory: {}", stagingDir);
        switch (PluginManagerService.getActionType(action)) {
            case EXPORT:
                Path path = plugin.exportData(pluginDataset);
                if (path != null) {
                    LOG.debug("Plugin policies exported to {}", path.toString());
                    jobContext.getJobContextMap().put(PLUGIN_STAGING_PATH, path.toString());
                }
                break;

            case IMPORT:
                String stagingPathStr = jobContext.getJobContextMap().get(PLUGIN_STAGING_PATH);
                LOG.debug("Plugin policies imported from {}", stagingPathStr);
                Path stagingPath = (StringUtils.isEmpty(stagingPathStr) || stagingPathStr.equals("null")) ? null
                        : new Path(stagingPathStr);
                plugin.importData(pluginDataset, stagingPath);
                break;

            default:
                throw new BeaconException("Job action type {} is not supported for plugin {}", action, pluginName);
        }
        setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS);
    }

    @Override
    public void cleanUp(JobContext jobContext) {

    }

    @Override
    public void recover(JobContext jobContext) {
        LOG.info("Recover policy instance: [{}]", jobContext.getJobInstanceId());
    }

    @Override
    public void interrupt() throws BeaconException {

    }
}
