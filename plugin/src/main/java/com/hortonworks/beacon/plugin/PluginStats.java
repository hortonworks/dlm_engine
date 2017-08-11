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


import com.hortonworks.beacon.exceptions.BeaconException;
import org.json.JSONObject;

/**
 * Provide beacon plugin statistics.
 */
public interface PluginStats {

    /**
     * Beacon plugins are expected to provide the metrics regarding their plugin as a JSON
     * object in the following format
     * [
     * "metric" : value,
     * ...
     * ]
     *
     * @return plugin statistics
     * @throws BeaconException
     */
    JSONObject getPluginStats() throws BeaconException;
}
