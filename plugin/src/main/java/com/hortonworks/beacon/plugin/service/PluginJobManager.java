/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.plugin.service;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.entity.util.EntityHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
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
public class PluginJobManager extends InstanceReplication implements BeaconJob {
    private static final Logger LOG = LoggerFactory.getLogger(PluginJobManager.class);
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
            throw new BeaconException("Plugin " + pluginName + " not registered. Cannot perform the job");
        }

        Plugin plugin = PluginManagerService.getPlugin(pluginName);
        Plugin.Status pluginStatus = plugin.getStatus();
        // To-DO: Do we throw exception?
        if (Plugin.Status.ACTIVE != pluginStatus) {
            throw new BeaconException("Plugin " + pluginName + " is in " + pluginStatus + " and not in active state");
        }

        String action = properties.getProperty(PluginJobProperties.JOBACTION_TYPE.getName());

        String dataset = properties.getProperty(PluginJobProperties.DATASET.getName());
        String datasetType = properties.getProperty(PluginJobProperties.DATASET_TYPE.getName());
        Cluster srcCluster = EntityHelper.getEntity(EntityType.CLUSTER, properties.getProperty(
                PluginJobProperties.SOURCE_CLUSTER.getName()));
        Cluster targetCluster = EntityHelper.getEntity(EntityType.CLUSTER, properties.getProperty(
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
                    LOG.info("No import needed for dataset: {}", pluginDataset);
                    return;
                }

                plugin.importData(pluginDataset, new Path(stagingPath));
                break;

            default:
                throw new BeaconException("Job action type " + action + " not supported for plugin " + pluginName);
        }
        setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS);
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {

    }

}
