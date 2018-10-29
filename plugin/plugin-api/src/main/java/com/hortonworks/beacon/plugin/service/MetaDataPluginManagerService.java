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

package com.hortonworks.beacon.plugin.service;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.plugin.PluginInfo;
import com.hortonworks.beacon.service.PluginManagerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Plugin Manager for managing plugins like Atlas, Ranger etc.
 */
public final class MetaDataPluginManagerService extends PluginManagerService<Plugin> {

    private static final Logger LOG =  LoggerFactory.getLogger(MetaDataPluginManagerService.class);

    @Override
    public Class getPluginServiceClassName() {
        return Plugin.class;
    }

    @Override
    /**
     * Overriding here as the registration mechanism has extra checks based on the outcome of
     * {@link Plugin#register()}
     */
    public void registerPlugins() throws BeaconException {
        for (Plugin plugin : pluginServiceLoader) {
            PluginInfo pluginInfo = plugin.register();
            if (pluginInfo == null) {
                throw new BeaconException("Plugin info cannot be null or empty. Registration failed");
            }
            if (Plugin.Status.INVALID == plugin.getStatus() || Plugin.Status.INACTIVE == plugin.getStatus()) {
                LOG.info("Plugin {} is in {} state. Not registering.", pluginInfo.getName(), plugin.getStatus());
                continue;
            }
            logPluginDetails(pluginInfo);
            if (!registeredPluginsMap.containsKey(pluginInfo.getName().toUpperCase())) {
                registeredPluginsMap.put(pluginInfo.getName().toUpperCase(), plugin);
                LOG.info("Plugin {} registered successfully.", pluginInfo.getName());
            }
        }
    }

    private static void logPluginDetails(PluginInfo pluginInfo) {
        LOG.debug("Registering plugin: {}", pluginInfo.getName());
        LOG.debug("Plugin dependencies: {}", pluginInfo.getDependencies());
        LOG.debug("Plugin description: {}", pluginInfo.getDescription());
        LOG.debug("Plugin version: {}", pluginInfo.getVersion());
        LOG.debug("Plugin ignore failures for plugin jobs: {}", pluginInfo.ignoreFailures());
    }


    public PluginAction getActionType(final String actionType) throws BeaconException {
        try {
            return PluginAction.valueOf(actionType.toUpperCase());
        } catch (IllegalArgumentException ex) {
            LOG.error("Action of type: {} is not supported", actionType);
            throw new BeaconException("Action of type: {} is not supported", actionType);
        }
    }
}
