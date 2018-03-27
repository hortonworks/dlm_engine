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

package com.hortonworks.beacon.plugin.ranger;

import java.io.File;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.hsqldb.lib.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.BeaconInfo;
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.plugin.PluginInfo;
import com.hortonworks.beacon.plugin.PluginStats;

/**
 * A simple plugin provider interface for DLM.
 * <p>
 * This is the V1 of the plugin infrastructure.   The plugins will be defined using
 * the interfaces defined as part of  <i>ServiceLoader</i> in JDK.   That is
 * the implementation of the plugins will be  packaged as a  jar file that is
 * available in the beacon server classpath.
 * <p>
 * On startup, beacon server will identify all implementations of the Beacon plugin
 * interface and load them.
 * <p>
 * In V1, we only support system plugins.   These plugins are Ranger and Atlas
 * and they have well defined orchestration properties
 * Always invoked before the actual data plugin (hive/hdfs) is invoked
 * Failure to invoke the plugin aborts the replication (should we relax this for
 * Atlas)
 */
public class BeaconRangerPluginImpl implements Plugin{

    /**
     * Get plugin status.   Valid statuses are ACTIVE, INACTIVE, INITIALIZING, ERROR
     */
    private static final String PLUGIN_NAME = "ranger";
    private static final Logger LOG = LoggerFactory.getLogger(BeaconRangerPluginImpl.class);
    private Plugin.Status pluginStatus=Plugin.Status.INACTIVE;
    /**
     * Register the plugin with beacon specific information.    The BeaconInfo object will provide
     * the beacon staging directory location and cluster name among others.  Beacon plugin system will
     * call this method on the plugin provider on discovery.   The plugin should make copy the info for
     * future usage.  The plugin should return the information on the plugin as a response.
     *
     * @param info
     * @return
     * @throws BeaconException
     */

    @Override
    public PluginInfo register(BeaconInfo info) throws BeaconException{
        if (!StringUtil.isEmpty(info.getCluster().getRangerEndpoint())) {
            pluginStatus=Plugin.Status.ACTIVE;
        }
        return getPluginDetails(info);
    }

    /**
     * Export the plugin specific data for the given <i>dataset</i> from the <i>srcCluster</i> to
     * the path <i>exportPath</i>.   The path returned is managed by the plugin.   Plugin can use
     * the Beacon provided staging dir to create subfolders to manage the plugin specific data, but
     * the lifecycle of the data is managed by the plugin.
     * There can be only one outstanding call to a plugin to export data related to a dataset.
     * Note that this call is invoked on the plugin in the targetCluster
     * A plugin can return an empty path to signify that there is no data to export.
     *
     * @param dataset
     * @return Path where the plugin data is returned.  Empty path means no data.
     * @throws BeaconException
     */
    @Override
    public Path exportData(DataSet dataset) throws BeaconException{
        RangerAdminRESTClient rangerAdminRESTClient = new RangerAdminRESTClient();
        LOG.info("Ranger policy export started");
        RangerExportPolicyList rangerExportPolicyList=rangerAdminRESTClient.exportRangerPolicies(dataset);
        List<RangerPolicy> rangerPolicies =rangerExportPolicyList.getPolicies();
        if (rangerPolicies.isEmpty()) {
            LOG.info("Ranger policy export request returned empty list or failed, Please refer Ranger admin logs.");
            rangerExportPolicyList=new RangerExportPolicyList();
        } else {
            rangerPolicies=rangerAdminRESTClient.removeMutilResourcePolicies(dataset, rangerPolicies);
        }
        LOG.info("Ranger policy export finished successfully");
        List<RangerPolicy> updatedRangerPolicies = rangerAdminRESTClient.addSingleDenyPolicies(dataset,
                rangerPolicies);
        updatedRangerPolicies = rangerAdminRESTClient.changeDataSet(dataset, updatedRangerPolicies);
        if (!CollectionUtils.isEmpty(updatedRangerPolicies)){
            rangerExportPolicyList.setPolicies(updatedRangerPolicies);
            LOG.info("Ranger policy import started");
            rangerAdminRESTClient.importRangerPolicies(dataset, rangerExportPolicyList);
        }
        return null;
    }

    /**
     * Import the plugin specific data for the given <i>dataset</i> from the <i>targetCluster</i> from
     * the path <i>exportedDataPath</i>
     * There can be only one outstanding call to a plugin to import data related to a dataset.
     * After a successful import, the plugin is responsible for cleanup of the data and the staging paths.
     *
     * @param dataset
     * @param exportedDataPath  Data that was exported by export command.
     * @return
     * @throws BeaconException
     */
    @Override
    public void importData(DataSet dataset, Path exportedDataPath)
            throws BeaconException{
    }

    /**
     * Return plugin specific information.
     *
     * @return Plugin info
     * @throws BeaconException
     */
    @Override
    public PluginInfo getInfo() throws BeaconException{
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
                Path path =  new Path(BeaconConfig.getInstance().getEngine().getPluginStagingPath());
                String stagingPath = path + File.separator + PLUGIN_NAME;
                return stagingPath;
            }

            @Override
            public boolean ignoreFailures() {
                return false;
            }
        };
        return info;
    }


    /**
     * Return plugin stats.
     *
     * @return Plugin stats as a JSON
     * @throws BeaconException
     */
    @Override
    public PluginStats getStats() throws BeaconException{
        return null;
    }

    /**
     * Get plugin status.   Valid statuses are ACTIVE, INACTIVE, INITIALIZING, ERROR
     *
     * @return Plugin status
     * @throws BeaconException
     */
    @Override
    public Status getStatus() throws BeaconException{
        return pluginStatus;
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
                String stagingPath = beaconInfo.getStagingDir() + File.separator + PLUGIN_NAME;
                return stagingPath;
            }

            @Override
            public boolean ignoreFailures() {
                return false;
            }
        };
        return info;
    }
}
