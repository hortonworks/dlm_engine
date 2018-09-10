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
package com.hortonworks.beacon.plugin.atlas;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.DataSet;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Atlas Proccesses, viz. Export & Import
 */
public abstract class AtlasProcess {
    protected static final Logger LOG = LoggerFactory.getLogger(AtlasProcess.class);
    private static final String CLUSTER_NAME_SEPARATOR = "$";

    private AtlasPluginStats pluginStats;

    private RESTClientBuilder builder;

    public AtlasProcess(RESTClientBuilder builder) {
        this.builder = builder;
    }

    protected void setBuilder(RESTClientBuilder builder) {
        this.builder = builder;
    }

    protected RESTClientBuilder getBuilder() {
        return this.builder;
    }

    public void setPluginStats(AtlasPluginStats pluginStats) {
        this.pluginStats = pluginStats;
    }

    protected void updateStats(String key, long value) {
        if (this.pluginStats == null) {
            return;
        }

        this.pluginStats.updateStats(key, value);
    }

    protected RESTClient getClient(Cluster cluster) throws BeaconException {
        String atlasEndpoint = cluster.getAtlasEndpoint();
        if (StringUtils.isEmpty(atlasEndpoint)) {
            LOG.error("dataSet.getTargetCluster().getAtlasEndpoint(): Was not defined. " + AtlasType.toJson(cluster));
            return null;
        }

        BeaconCluster beaconCluster = new BeaconCluster(cluster);
        String knoxGatewayURL = beaconCluster.getKnoxGatewayURL();
        return builder.knoxBaseUrl(knoxGatewayURL).baseUrl(atlasEndpoint).create();
    }

    protected String getAtlasServerName(Cluster cluster) {
        String clusterName = cluster.getName();
        String atlasCluserName = clusterName;
        if (org.apache.commons.lang.StringUtils.contains(clusterName, CLUSTER_NAME_SEPARATOR)) {
            atlasCluserName = org.apache.commons.lang.StringUtils.split(clusterName, CLUSTER_NAME_SEPARATOR)[1];
        }

        return atlasCluserName;
    }

    public String getEntityGuid(Cluster cluster,
                                String typeName, String attributeName, String attributeValue) {
        try {
            return getClient(cluster).getEntityGuid(typeName, attributeName, attributeValue);
        } catch (BeaconException e) {
            LOG.error("getEntityGuid: failed for: {} - {}:{}", typeName, attributeName, attributeValue);
        }

        return null;
    }

    public abstract Path run(DataSet dataset, Path stagingDir, AtlasPluginStats pluginStats) throws BeaconException;
}
