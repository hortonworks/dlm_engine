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
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.plugin.PluginStatus;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 *  Plugin Job manger.
 */
public class PluginJobManager implements BeaconJob {
    private static final Logger LOG = LoggerFactory.getLogger(PluginJobManager.class);
    private static final String PLUGIN_STAGING_PATH = "PLUGIN_STAGINGPATH";

    private Properties properties;

    public PluginJobManager(ReplicationJobDetails details) {
        this.properties = details.getProperties();
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {

    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        // get the plugin name
        String pluginName = properties.getProperty(PluginJobProperties.JOB_TYPE.getName());
        if (!PluginManagerService.isPluginRegistered(pluginName)) {
            throw new BeaconException("Plugin " + pluginName + " not registered. Cannot perform the job");
        }

        Plugin plugin = PluginManagerService.getPlugin(pluginName);
        PluginStatus.Status pluginStatus = plugin.getStatus().getStatus();
        // To-DO: Do we throw exception?
        if (PluginStatus.Status.ACTIVE != pluginStatus) {
            throw new BeaconException("Plugin " + pluginName + " is in " + pluginStatus + " and not in active state");
        }

        String action = properties.getProperty(PluginJobProperties.JOBACTION_TYPE.getName());

        String dataset = properties.getProperty(PluginJobProperties.DATASET.getName());
        String datasetType = properties.getProperty(PluginJobProperties.DATASET_TYPE.getName());
        DataSet pluginDataset = new DatasetImpl(dataset, DataSet.DataSetType.valueOf(datasetType.toUpperCase()));

        switch (PluginManagerService.getActionType(action)) {
            case EXPORT:
                String clusterName = properties.getProperty(PluginJobProperties.SOURCE_CLUSTER.getName());
                Cluster srcCluster = EntityHelper.getEntity(EntityType.CLUSTER, clusterName);
                Path path = plugin.exportData(srcCluster, pluginDataset);
                jobContext.getJobContextMap().put(PLUGIN_STAGING_PATH, path.toString());
                break;

            case IMPORT:
                String stagingPath = jobContext.getJobContextMap().get(PLUGIN_STAGING_PATH);
                if (StringUtils.isBlank(stagingPath)) {
                    LOG.info("No import needed for dataset: {}", pluginDataset);
                    return;
                }
                clusterName = properties.getProperty(PluginJobProperties.TARGET_CLUSTER.getName());
                Cluster targetCluster = EntityHelper.getEntity(EntityType.CLUSTER, clusterName);

                plugin.importData(targetCluster, pluginDataset, new Path(stagingPath));
                break;

            default:
                throw new BeaconException("Job action type " + action + " not supported for plugin " + pluginName);
        }
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {

    }

    @Override
    public String getJobExecutionContextDetails() throws BeaconException {
        return null;
    }
}
