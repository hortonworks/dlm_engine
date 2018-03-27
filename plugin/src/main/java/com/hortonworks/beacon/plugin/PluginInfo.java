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
