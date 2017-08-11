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
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.BeaconInfo;
import org.apache.hadoop.fs.Path;


/**
 * Beacon Info implementation.
 */
public class BeaconInfoImpl implements BeaconInfo {
    private static final Path BEACON_STAGING_PATH =  new Path(BeaconConfig.getInstance().
            getEngine().getPluginStagingPath());

    @Override
    public Cluster getCluster() throws BeaconException {
        return ClusterHelper.getLocalCluster();
    }

    @Override
    public Path getStagingDir() throws BeaconException {
        return BEACON_STAGING_PATH;
    }
}
