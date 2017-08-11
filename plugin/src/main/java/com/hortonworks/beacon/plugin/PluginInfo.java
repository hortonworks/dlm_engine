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

/**
 * This defines the plugin information that the plugin provides.
 */
public interface PluginInfo {

    /**
     * Return the plugin name.
     *
     * @return name
     */
    String getName();

    /**
     * Return the plugin version.
     *
     * @return version
     */
    String getVersion();

    /**
     * Return the plugin description.   This can be more descriptive as to the plugin operations etc
     *
     * @return description
     */
    String getDescription();

    /**
     * Dependencies of the plugin as a comma separated components in the format component-version.
     * Currently just logged by the plugin manager on discovery.
     *
     * @return dependencies.
     */
    String getDependencies();

    /**
     * Return the absolute path of the plugin staging directory.   This must be owned and managed by the
     * plugin.
     *
     * @return staging dir
     */
    String getStagingDir() throws BeaconException;

    /**
     * Return true if job should continue running other actions on plugin failure else return false.
     *
     * @return ignore failures or not
     */
    boolean ignoreFailures();
}
