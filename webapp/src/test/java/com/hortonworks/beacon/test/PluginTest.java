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

package com.hortonworks.beacon.test;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.BeaconInfo;
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.plugin.PluginInfo;
import com.hortonworks.beacon.plugin.PluginStats;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Implementation of Plugin for IT purpose.
 */
public class PluginTest implements Plugin {
    private static String stagingPath;
    private static final String PLUGIN_NAME = "ranger";
    // Used only for Beacon IT purpose
    private static boolean allowPlugin = false;

    @Override
    public PluginInfo register(BeaconInfo info) throws BeaconException {
        Properties clusterProperties = info.getCluster().getCustomProperties();
        if (clusterProperties != null) {
            String allowPluginStr = (String) clusterProperties.get("allowPluginsOnThisCluster");
            if (StringUtils.isNotBlank(allowPluginStr) && allowPluginStr.equalsIgnoreCase("true")) {
                allowPlugin = true;
            }
        }

        PluginInfo pluginInfo = getPluginDetails(info);
        // allowPlugin used only for Beacon IT purpose
        if (allowPlugin) {
            // Create staging path on target
            FileSystem targetFS = FSUtils.getFileSystem(info.getCluster().getFsEndpoint(), new Configuration(), false);
            Path exportPath;
            try {
                exportPath = new Path(pluginInfo.getStagingDir());
                targetFS.mkdirs(exportPath);
            } catch (IOException e) {
                throw new BeaconException(e);
            }
        }

        return pluginInfo;
    }

    @Override
    public Path exportData(DataSet dataset) throws BeaconException {
        if (!allowPlugin) {
            return null;
        }
        Cluster srcCluster = dataset.getSourceCluster();
        FileSystem sourceFs = FSUtils.getFileSystem(srcCluster.getFsEndpoint(), new Configuration(), false);
        String name = new Path(dataset.getDataSet()).getName();
        Path exportPath;
        try {
            exportPath = new Path(stagingPath, name);
            sourceFs.mkdirs(exportPath);
            sourceFs.createNewFile(new Path(exportPath, "ranger.txt"));
        } catch (IOException e) {
            throw new BeaconException(e);
        }
        Path tmpPath = new Path(srcCluster.getFsEndpoint(), exportPath);
        return tmpPath;
    }

    @Override
    public void importData(DataSet dataset, Path exportedDataPath) throws BeaconException {
        if (!allowPlugin) {
            return;
        }
        Cluster targetCluster = dataset.getTargetCluster();
        Path targetPath = new Path(targetCluster.getFsEndpoint(), stagingPath);
        // Do distcp
        invokeCopy(exportedDataPath, targetPath);
        FileSystem targetFS = FSUtils.getFileSystem(targetCluster.getFsEndpoint(), new Configuration(), false);
        try {
            /* TODO - DO we have to delete ranger.txt and this file after test run */
            targetFS.createNewFile(new Path(Path.getPathWithoutSchemeAndAuthority(exportedDataPath), "_SUCCESS"));
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public PluginInfo getInfo() throws BeaconException {
        return null;
    }

    @Override
    public PluginStats getStats() throws BeaconException {
        return null;
    }

    @Override
    public Status getStatus() throws BeaconException {
        return Status.ACTIVE;
    }

    private static PluginInfo getPluginDetails(final BeaconInfo beaconInfo) {
        PluginInfo info = new PluginInfo() {
            @Override
            public String getName() {
                return "Ranger";
            }

            @Override
            public String getVersion() {
                return "1.0";
            }

            @Override
            public String getDescription() {
                return "Ranger Plugin";
            }

            @Override
            public String getDependencies() {
                return null;
            }

            @Override
            public String getStagingDir() throws BeaconException {
                stagingPath = beaconInfo.getStagingDir() + File.separator + PLUGIN_NAME;
                return stagingPath;
            }

            @Override
            public boolean ignoreFailures() {
                return false;
            }
        };
        return info;
    }

    private static void invokeCopy(Path sourceStagingUri, Path targetPath) throws BeaconException {
        Configuration conf = new Configuration();
        Job job = null;
        try {
            DistCpOptions options = getDistCpOptions(sourceStagingUri, targetPath);

            DistCp distCp = new DistCp(conf, options);
            job = distCp.createAndSubmitJob();
            distCp.waitForJobCompletion(job);
        } catch (InterruptedException e) {
            if (job != null) {
                try {
                    job.killJob();
                } catch (IOException ioe) {
                    throw new BeaconException(ioe);
                }
            }
            throw new BeaconException(e);
        } catch (Exception e) {
            throw new BeaconException(e);
        }
    }

    private static DistCpOptions getDistCpOptions(Path sourceStagingUri,
                                                  Path targetPath) throws BeaconException, IOException {
        List<Path> sourceUris = new ArrayList<>();
        sourceUris.add(sourceStagingUri);

        DistCpOptions distcpOptions = new DistCpOptions(sourceUris, targetPath);
        distcpOptions.setBlocking(true);

        return distcpOptions;
    }

    public static String getPluginName() {
        return PLUGIN_NAME;
    }
}
