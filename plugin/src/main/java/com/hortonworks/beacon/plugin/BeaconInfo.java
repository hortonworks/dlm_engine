/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */


package com.hortonworks.beacon.plugin;


import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.fs.Path;

/**
 * BeaconInfo to be used by the plugin.
 */
public interface BeaconInfo {

    /**
     * Return the current cluster of beacon engine.
     *
     * @return cluster
     * @throws BeaconException
     */
    Cluster getCluster() throws BeaconException;

    /**
     * Return the staging directory that beacon uses.   Plugins can create subdirectories
     * with appropriate permissions underneath this directory.   This directory will be owned by beacon
     * and writable by the hadoop group (the group for services).
     *
     * @return Staging directory
     * @throws BeaconException
     */
    Path getStagingDir() throws BeaconException;

}
