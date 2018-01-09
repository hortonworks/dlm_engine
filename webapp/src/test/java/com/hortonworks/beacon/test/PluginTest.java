/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.test;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.BeaconInfo;
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.plugin.PluginInfo;
import com.hortonworks.beacon.plugin.PluginStats;
import com.hortonworks.beacon.plugin.service.PluginManagerService;
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
    private static final String PLUGIN_NAME = PluginManagerService.DEFAULT_PLUGIN;
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
        String name = new Path(dataset.getSourceDataSet()).getName();
        Path exportPath;
        try {
            exportPath = new Path(stagingPath, name);
            sourceFs.mkdirs(exportPath);
            sourceFs.createNewFile(new Path(exportPath, "sample.txt"));
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
            /* TODO - DO we have to delete sample.txt and this file after test run */
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
                return PLUGIN_NAME;
            }

            @Override
            public String getVersion() {
                return "1.0";
            }

            @Override
            public String getDescription() {
                return "Ranger Test Plugin";
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
