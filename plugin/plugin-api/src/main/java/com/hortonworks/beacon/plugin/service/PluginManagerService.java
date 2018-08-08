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

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.plugin.PluginInfo;
import com.hortonworks.beacon.service.BeaconService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;

/**
 *  Plugin Manager for managing plugins.
 */
public final class PluginManagerService implements BeaconService {
    private static final Logger LOG = LoggerFactory.getLogger(PluginManagerService.class);

    private static ServiceLoader<Plugin> pluginServiceLoader;
    private static Map<String, Plugin> registeredPluginsMap = new HashMap<>();
    public static final String DEFAULT_PLUGIN = "RANGER";

    private static final Map<String, Integer> DEFAULTPLUGINSORDERMAP = new HashMap<String, Integer>() {
        {
            put(DEFAULT_PLUGIN, 1);
            put("ATLAS", 2);
        }
    };

    enum DefaultPluginActions {
        EXPORT("EXPORT"),
        IMPORT("IMPORT");

        private final String name;

        DefaultPluginActions(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }


    @Override
    public void init() throws BeaconException {
        loadPlugins();
        try {
            RequestContext.setInitialValue();
            Cluster localCluster = ClusterHelper.getLocalCluster();
            if (localCluster != null && localCluster.isLocal()) {
                registerPlugins();
            }
        } catch (NoSuchElementException e) {
            LOG.info("Local cluster is not registered yet. Plugins will not be registered.");
        } finally {
            RequestContext.get().clear();
        }
    }

    @Override
    public void destroy() {
    }

    private void loadPlugins() throws BeaconException {
        Class pluginServiceClassName = Plugin.class;
        pluginServiceLoader = ServiceLoader.load(pluginServiceClassName);
        Iterator<Plugin> pluginServices = pluginServiceLoader.iterator();
        if (!pluginServices.hasNext()) {
            LOG.info("Cannot find implementation for: {}", pluginServiceClassName);
        }
    }

    /* Register all the plugins once the local cluster is submitted as Plugin need to know the cluster details */
    public void registerPlugins() throws BeaconException {
        for (Plugin plugin : pluginServiceLoader) {
            PluginInfo pluginInfo = plugin.register(new BeaconInfoImpl());
            if (pluginInfo == null) {
                throw new BeaconException("Plugin info cannot be null or empty. Registration failed");
            }
            if (Plugin.Status.INVALID == plugin.getStatus() || Plugin.Status.INACTIVE == plugin.getStatus()) {
                if (DEFAULT_PLUGIN.equalsIgnoreCase(pluginInfo.getName())) {
                    LOG.info("Ranger plugin is in invalid state. Not registering any other plugins.",
                        pluginInfo.getName());
                    break;
                }
                LOG.info("Plugin {} is in {} state. Not registering.", pluginInfo.getName(), plugin.getStatus());
                continue;
            }
            logPluginDetails(pluginInfo);
            registeredPluginsMap.put(pluginInfo.getName().toUpperCase(), plugin);
        }
    }

    private static void logPluginDetails(PluginInfo pluginInfo) throws BeaconException {
        LOG.debug("Registering plugin: {}", pluginInfo.getName());
        LOG.debug("Plugin dependencies: {}", pluginInfo.getDependencies());
        LOG.debug("Plugin description: {}", pluginInfo.getDescription());
        LOG.debug("Plugin staging dir: {}", pluginInfo.getStagingDir());
        LOG.debug("Plugin version: {}", pluginInfo.getVersion());
        LOG.debug("Plugin ignore failures for plugin jobs: {}", pluginInfo.ignoreFailures());
    }

    public Plugin.Status getStatus(final String pluginName) throws BeaconException {
        if (StringUtils.isBlank(pluginName)) {
            throw new BeaconException("pluginName cannot be null or empty");
        }

        Plugin plugin = registeredPluginsMap.get(pluginName.toUpperCase());
        if (plugin == null) {
            throw new BeaconException("No such plugin {} has been registered with beacon", pluginName);
        }

        return plugin.getStatus();
    }

    public static List<String> getRegisteredPlugins() {
        List<String> pluginList = new ArrayList<>();
        if (registeredPluginsMap != null && !registeredPluginsMap.isEmpty()) {
            for (String pluginName : registeredPluginsMap.keySet()) {
                pluginList.add(pluginName);
            }
        }

        return pluginList;
    }

    public static boolean isPluginRegistered(final String pluginName) throws BeaconException {
        if (StringUtils.isBlank(pluginName)) {
            throw new BeaconException("pluginName cannot be null or empty");
        }
        return (registeredPluginsMap.get(pluginName.toUpperCase()) == null ? false : true);
    }

    static Plugin getPlugin(final String pluginName) throws BeaconException {
        if (isPluginRegistered(pluginName)) {
            return registeredPluginsMap.get(pluginName.toUpperCase());
        } else {
            throw new BeaconException("No such plugin {} has been registered with beacon", pluginName);
        }

    }

    static Integer getPluginOrder(final String pluginName) {
        return DEFAULTPLUGINSORDERMAP.get(pluginName);
    }

    static DefaultPluginActions getActionType(final String actionType) throws BeaconException {
        try {
            return DefaultPluginActions.valueOf(actionType.toUpperCase());
        } catch (IllegalArgumentException ex) {
            LOG.error("Action of type: {} is not supported", actionType);
            throw new BeaconException("Action of type: {} is not supported", actionType);
        }
    }
}
