/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.plugin.service;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.plugin.PluginInfo;
import com.hortonworks.beacon.plugin.PluginStats;
import com.hortonworks.beacon.plugin.PluginStatus;
import com.hortonworks.beacon.service.BeaconService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 *  Plugin Manager for managing plugins.
 */
public class PluginManagerService implements BeaconService {
    private static final Logger LOG = LoggerFactory.getLogger(PluginManagerService.class);

    private static Map<String, Plugin> registeredPluginsMap = new HashMap<>();

    private static final Map<String, Integer> DEFAULTPLUGINSORDERMAP = new HashMap<String, Integer>() {
        {
            put("RANGER", 1);
            put("ATLAS", 2);
        }
    };

    enum DefaultPluginActions {
        EXPORT("export"),
        IMPORT("import");

        private final String name;

        DefaultPluginActions(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }


    @Override
    public String getName() {
        return this.getClass().getName();
    }

    @Override
    public void init() throws BeaconException {
        LOG.info("Init PluginManagerService");
        loadAndRegisterPlugins();
    }

    @Override
    public void destroy() throws BeaconException {
    }

    private void loadAndRegisterPlugins() throws BeaconException {
        Class pluginServiceClassName = Plugin.class;
        ServiceLoader<Plugin> pluginServiceLoader = ServiceLoader.load(pluginServiceClassName);
        Iterator<Plugin> pluginServices = pluginServiceLoader.iterator();
        if (!pluginServices.hasNext()) {
            LOG.info("Cannot find implementation for: {}", pluginServiceClassName);
            return;
        }

        while (pluginServices.hasNext()) {
            Plugin plugin = pluginServices.next();

            PluginInfo pluginInfo = plugin.register(new BeaconInfoImpl());
            logPluginDetails(pluginInfo);

            registeredPluginsMap.put(pluginInfo.getName(), plugin);
        }
    }

    private static void logPluginDetails(PluginInfo pluginInfo) throws BeaconException {
        LOG.info("Registering plugin: {}", pluginInfo.getName());
        LOG.info("Plugin dependencies: {}", pluginInfo.getDependencies());
        LOG.info("Plugin description: {}", pluginInfo.getDescription());
        LOG.info("Plugin staging dir: {}", pluginInfo.getStagingDir());
        LOG.info("Plugin version: {}", pluginInfo.getVersion());
        LOG.info("Plugin ignore failures for plugin jobs: {}", pluginInfo.ignoreFailures());
    }

    public PluginInfo getInfo(final String pluginName) throws BeaconException {
        if (StringUtils.isBlank(pluginName)) {
            throw new BeaconException("plugin name cannot be null or empty");
        }

        Plugin plugin = registeredPluginsMap.get(pluginName);
        if (plugin == null) {
            throw new BeaconException("No such plugin " + pluginName + " has been registered with Beacon");
        }
        return plugin.getInfo();
    }

    public PluginStats getStats(final String pluginName) throws BeaconException {
        if (StringUtils.isBlank(pluginName)) {
            throw new BeaconException("plugin name cannot be null or empty");
        }

        Plugin plugin = registeredPluginsMap.get(pluginName);
        if (plugin == null) {
            throw new BeaconException("No such plugin " + pluginName + " has been registered with Beacon");
        }
        return plugin.getStats();
    }

    public PluginStatus getStatus(final String pluginName) throws BeaconException {
        if (StringUtils.isBlank(pluginName)) {
            throw new BeaconException("plugin name cannot be null or empty");
        }

        Plugin plugin = registeredPluginsMap.get(pluginName);
        if (plugin == null) {
            throw new BeaconException("No such plugin " + pluginName + " has been registered with Beacon");
        }

        return plugin.getStatus();
    }

    static List<String> getRegisteredPlugins() {
        List<String> pluginList = new ArrayList<>();
        if (registeredPluginsMap == null || registeredPluginsMap.isEmpty()) {
            LOG.info("No registered plugins");
        } else {
            for (String pluginName : registeredPluginsMap.keySet()) {
                pluginList.add(pluginName);
            }
        }

        return pluginList;
    }

    static boolean isPluginRegistered(final String pluginName) {
        return (registeredPluginsMap.get(pluginName) == null ? false : true);
    }

    static Plugin getPlugin(final String pluginName) throws BeaconException {
        if (isPluginRegistered(pluginName)) {
            return registeredPluginsMap.get(pluginName);
        } else {
            throw new BeaconException("No such plugin " + pluginName + " has been registered with Beacon");
        }

    }

    static Integer getPluginOrder(final String pluginName) {
        return DEFAULTPLUGINSORDERMAP.get(pluginName);
    }

    static DefaultPluginActions getActionType(final String actionType) throws BeaconException {
        try {
            return DefaultPluginActions.valueOf(actionType.toUpperCase());
        } catch (IllegalArgumentException ex) {
            LOG.error("{} is not valid action type", actionType);
            throw new BeaconException("Action of type (" + actionType + " is not supported) ");
        }
    }
}
