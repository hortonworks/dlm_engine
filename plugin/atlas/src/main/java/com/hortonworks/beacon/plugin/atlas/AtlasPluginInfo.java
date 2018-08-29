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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.BeaconInfo;
import com.hortonworks.beacon.plugin.PluginInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Atlas Plugin Info.
 */
public class AtlasPluginInfo implements PluginInfo {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPluginInfo.class);

    public static final String PLUGIN_NAME = "Atlas";
    public static final String ATLAS_PLUGIN_VERSION = "0.8";
    public static final String ATLAS_PLUGIN_DESCRIPTION = "Beacon Atlas Plugin";

    private static final String STAGING_DIRECTORY_NAME_FORMAT = "%s%s%s";

    private final BeaconInfo beaconInfo;

    public AtlasPluginInfo(BeaconInfo beaconInfo) {
        this.beaconInfo = beaconInfo;
    }

    @Override
    public String getName() {
        return PLUGIN_NAME;
    }

    @Override
    public String getVersion() {
        return ATLAS_PLUGIN_VERSION;
    }

    @Override
    public String getDescription() {
        return ATLAS_PLUGIN_DESCRIPTION;
    }

    @Override
    public String getDependencies() {
        return null;
    }

    @Override
    public String getStagingDir() throws BeaconException {
        if (beaconInfo == null) {
            return "";
        }

        String s = String.format(STAGING_DIRECTORY_NAME_FORMAT,
                beaconInfo.getStagingDir(), File.separator, BeaconAtlasPlugin.getPluginName());
        debugDisplay(s);
        return s;
    }

    private void debugDisplay(String s) {
        if (!LOG.isDebugEnabled()) {
            return;
        }

        LOG.debug(s);
    }

    @Override
    public boolean ignoreFailures() {
        return false;
    }
}
