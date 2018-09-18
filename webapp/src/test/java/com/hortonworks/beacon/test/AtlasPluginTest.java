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
package com.hortonworks.beacon.test;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.plugin.PluginInfo;
import com.hortonworks.beacon.plugin.PluginStats;
import com.hortonworks.beacon.plugin.atlas.AtlasPluginInfo;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of Plugin for IT purpose.
 */
public class AtlasPluginTest implements Plugin {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPluginTest.class);
    private static String stagingPath = BeaconConfig.getInstance().getEngine().getPluginStagingPath();
    // Used only for Beacon IT purpose

    @Override
    public PluginInfo register() throws BeaconException {
        PluginInfo pluginInfo = getPluginDetails();
        return pluginInfo;
    }

    @Override
    public Path exportData(DataSet dataset) throws BeaconException {
        LOG.info("Atlas policy export started");
        Cluster srcCluster = dataset.getSourceCluster();
        /**
         * Returning dummy path in case of cloud to hdfs replication as srcCluster will
         * not be defined during policy creation.
         */
        if (srcCluster == null) {
            return new Path("dummy");
        }
        FileSystem sourceFs = FSUtils.getFileSystem(srcCluster.getFsEndpoint(), new Configuration());
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
        LOG.debug("Export path: {}", tmpPath);
        LOG.info("Atlas policy export finished");
        return tmpPath;
    }

    @Override
    public void importData(DataSet dataset, Path exportedDataPath) throws BeaconException {
        LOG.info("Atlas policy import started");
        Cluster targetCluster = dataset.getTargetCluster();
        /**
         * Returning in case of hdfs to cloud replication as tgtCluster will
         * not be defined during policy creation.
         */
        if (targetCluster == null || exportedDataPath == null || targetCluster.getFsEndpoint() == null) {
            return;
        }
        FileSystem targetFS = FSUtils.getFileSystem(targetCluster.getFsEndpoint(), new Configuration());
        try {
            /* TODO - DO we have to delete sample.txt and this file after test run */
            targetFS.createNewFile(new Path(Path.getPathWithoutSchemeAndAuthority(exportedDataPath), "_SUCCESS"));
        } catch (IOException e) {
            throw new BeaconException(e);
        }
        LOG.info("Atlas policy import finished");
    }

    @Override
    public boolean isEnabled(String cluster) throws BeaconException {
        return true;
    }

    @Override
    public PluginInfo getInfo() throws BeaconException {
        return getPluginDetails();
    }

    @Override
    public PluginStats getStats() throws BeaconException {
        return null;
    }

    @Override
    public Status getStatus() throws BeaconException {
        return Status.ACTIVE;
    }

    private static PluginInfo getPluginDetails() {
        PluginInfo info = new AtlasPluginInfo();
        return info;
    }
}
