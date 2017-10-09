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
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.plugin.Plugin;
import com.hortonworks.beacon.plugin.PluginInfo;
import com.hortonworks.beacon.plugin.PluginStats;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.service.BeaconService;
import org.apache.commons.lang3.StringUtils;

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
    private static final BeaconLog LOG = BeaconLog.getLog(PluginManagerService.class);
    public static final String SERVICE_NAME = PluginManagerService.class.getName();

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
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws BeaconException {
        loadPlugins();
        try {
            Cluster localCluster = ClusterHelper.getLocalCluster();
            if (localCluster != null && localCluster.isLocal()) {
                registerPlugins();
            }
        } catch (NoSuchElementException e) {
            LOG.info(MessageCode.PLUG_000043.name());
        }
    }

    @Override
    public void destroy() throws BeaconException {
    }

    private void loadPlugins() throws BeaconException {
        Class pluginServiceClassName = Plugin.class;
        pluginServiceLoader = ServiceLoader.load(pluginServiceClassName);
        Iterator<Plugin> pluginServices = pluginServiceLoader.iterator();
        if (!pluginServices.hasNext()) {
            LOG.info(MessageCode.PLUG_000009.name(), pluginServiceClassName);
        }
    }

    /* Register all the plugins once the local cluster is submitted as Plugin need to know the cluster details */
    public void registerPlugins() throws BeaconException {
        for (Plugin plugin : pluginServiceLoader) {
            PluginInfo pluginInfo = plugin.register(new BeaconInfoImpl());
            if (pluginInfo == null) {
                throw new BeaconException(MessageCode.PLUG_000005.name());
            }
            if (Plugin.Status.INVALID == plugin.getStatus() || Plugin.Status.INACTIVE == plugin.getStatus()) {
                if (DEFAULT_PLUGIN.equalsIgnoreCase(pluginInfo.getName())) {
                    LOG.info(MessageCode.PLUG_000010.name(), pluginInfo.getName());
                    break;
                }
                LOG.info(MessageCode.PLUG_000011.name(), pluginInfo.getName(), plugin.getStatus());
                continue;
            }
            logPluginDetails(pluginInfo);
            registeredPluginsMap.put(pluginInfo.getName().toUpperCase(), plugin);
        }
    }

    private static void logPluginDetails(PluginInfo pluginInfo) throws BeaconException {
        LOG.debug(MessageCode.PLUG_000012.name(), pluginInfo.getName());
        LOG.debug(MessageCode.PLUG_000013.name(), pluginInfo.getDependencies());
        LOG.debug(MessageCode.PLUG_000014.name(), pluginInfo.getDescription());
        LOG.debug(MessageCode.PLUG_000015.name(), pluginInfo.getStagingDir());
        LOG.debug(MessageCode.PLUG_000016.name(), pluginInfo.getVersion());
        LOG.debug(MessageCode.PLUG_000017.name(), pluginInfo.ignoreFailures());
    }

    public PluginInfo getInfo(final String pluginName) throws BeaconException {
        if (StringUtils.isBlank(pluginName)) {
            throw new BeaconException(MessageCode.COMM_010008.name(), "plugin name");
        }

        Plugin plugin = registeredPluginsMap.get(pluginName.toUpperCase());
        if (plugin == null) {
            throw new BeaconException(MessageCode.PLUG_000006.name(), pluginName);
        }
        return plugin.getInfo();
    }

    public PluginStats getStats(final String pluginName) throws BeaconException {
        if (StringUtils.isBlank(pluginName)) {
            throw new BeaconException(MessageCode.COMM_010008.name(), "plugin name");
        }

        Plugin plugin = registeredPluginsMap.get(pluginName.toUpperCase());
        if (plugin == null) {
            throw new BeaconException(MessageCode.PLUG_000006.name(), pluginName);
        }
        return plugin.getStats();
    }

    public Plugin.Status getStatus(final String pluginName) throws BeaconException {
        if (StringUtils.isBlank(pluginName)) {
            throw new BeaconException(MessageCode.COMM_010008.name(), "plugin name");
        }

        Plugin plugin = registeredPluginsMap.get(pluginName.toUpperCase());
        if (plugin == null) {
            throw new BeaconException(MessageCode.PLUG_000006.name(), pluginName);
        }

        return plugin.getStatus();
    }

    public static List<String> getRegisteredPlugins() {
        List<String> pluginList = new ArrayList<>();
        if (registeredPluginsMap == null || registeredPluginsMap.isEmpty()) {
            LOG.info(MessageCode.PLUG_000018.name());
        } else {
            for (String pluginName : registeredPluginsMap.keySet()) {
                pluginList.add(pluginName);
            }
        }

        return pluginList;
    }

    public static boolean isPluginRegistered(final String pluginName) throws BeaconException {
        if (StringUtils.isBlank(pluginName)) {
            throw new BeaconException(MessageCode.COMM_010008.name(), "plugin name");
        }
        return (registeredPluginsMap.get(pluginName.toUpperCase()) == null ? false : true);
    }

    static Plugin getPlugin(final String pluginName) throws BeaconException {
        if (isPluginRegistered(pluginName)) {
            return registeredPluginsMap.get(pluginName.toUpperCase());
        } else {
            throw new BeaconException(MessageCode.PLUG_000006.name(), pluginName);
        }

    }

    static Integer getPluginOrder(final String pluginName) {
        return DEFAULTPLUGINSORDERMAP.get(pluginName);
    }

    static DefaultPluginActions getActionType(final String actionType) throws BeaconException {
        try {
            return DefaultPluginActions.valueOf(actionType.toUpperCase());
        } catch (IllegalArgumentException ex) {
            LOG.error(MessageCode.COMM_010009.name(), "Action of", actionType);
            throw new BeaconException(MessageCode.COMM_010009.name(), "Action of", actionType);
        }
    }
}
