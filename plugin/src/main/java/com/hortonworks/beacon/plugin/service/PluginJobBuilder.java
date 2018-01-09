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

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.ReplicationPolicyProperties;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.replication.JobBuilder;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Plugin JobBuilder.
 */
public class PluginJobBuilder extends JobBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(PluginJobBuilder.class);
    private static final String JOB_TYPE = ReplicationType.PLUGIN.name();

    @Override
    public List<ReplicationJobDetails> buildJob(ReplicationPolicy policy) throws BeaconException {
        List<ReplicationJobDetails> jobList = new ArrayList<>();
        if (!Services.get().isRegistered(PluginManagerService.SERVICE_NAME)) {
            return jobList;
        }

        if (!PluginManagerService.isPluginRegistered(PluginManagerService.DEFAULT_PLUGIN)) {
            // If ranger is not registered then no other plugin's are considered.
            LOG.info("Ranger plugin is not registered. Not adding any plugin jobs to add.");
            return jobList;
        }

        ReplicationType replType = ReplicationHelper.getReplicationType(policy.getType());
        if (ReplicationType.FS == replType) {
            // If dataset is HCFS, no plugin jobs
            if (PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
                return jobList;
            }
        }

        List<ReplicationJobDetails> nonPriorityJobList = new ArrayList<>();
        Map<Integer, List<ReplicationJobDetails>> priorityJobMap = new TreeMap<>();

        for (String pluginName : PluginManagerService.getRegisteredPlugins()) {
            for (PluginManagerService.DefaultPluginActions action
                    : PluginManagerService.DefaultPluginActions.values()) {
                ReplicationJobDetails jobDetails = buildReplicationJobDetails(policy, pluginName, action.getName());
                Integer order = PluginManagerService.getPluginOrder(pluginName);
                if (order == null) {
                    nonPriorityJobList.add(jobDetails);
                } else {
                    List<ReplicationJobDetails> jobs = new ArrayList<>();
                    if (priorityJobMap.get(order) != null) {
                        jobs = priorityJobMap.get(order);
                    }
                    jobs.add(jobDetails);
                    priorityJobMap.put(order, jobs);
                }
            }
        }

        if (!priorityJobMap.isEmpty()) {
            for (List<ReplicationJobDetails> jobs : priorityJobMap.values()) {
                jobList.addAll(jobs);
            }
        }

        if (!nonPriorityJobList.isEmpty()) {
            jobList.addAll(nonPriorityJobList);
        }

        return jobList;
    }

    private static ReplicationJobDetails buildReplicationJobDetails(final ReplicationPolicy policy,
                                                                    final String pluginType,
                                                                    final String actionType) throws BeaconException {
        Properties props = buildPluginProperties(policy, pluginType, actionType);
        String identifier = pluginType + actionType;
        return new ReplicationJobDetails(identifier, policy.getName(), JOB_TYPE, props);
    }

    public static Properties buildPluginProperties(ReplicationPolicy policy, String pluginType, String actionType)
            throws BeaconException {
        Map<String, String> map = new HashMap<>();

        map.put(PluginJobProperties.JOB_NAME.getName(), policy.getName());
        map.put(PluginJobProperties.JOB_TYPE.getName(), pluginType);
        map.put(PluginJobProperties.JOBACTION_TYPE.getName(), actionType);
        map.put(PluginJobProperties.SOURCE_DATASET.getName(), policy.getSourceDataset());
        map.put(PluginJobProperties.TARGET_DATASET.getName(), policy.getTargetDataset());
        map.put(PluginJobProperties.SOURCE_CLUSTER.getName(), policy.getSourceCluster());
        map.put(PluginJobProperties.TARGET_CLUSTER.getName(), policy.getTargetCluster());
        map.put(PluginJobProperties.DATASET_TYPE.getName(), getPluginDatsetType(policy.getType()));
        map.put(ReplicationPolicyProperties.RETRY_DELAY.getName(), String.valueOf(policy.getRetry().getDelay()));
        map.put(ReplicationPolicyProperties.RETRY_ATTEMPTS.getName(), String.valueOf(policy.getRetry().getAttempts()));

        Properties props = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            props.setProperty(entry.getKey(), entry.getValue());
        }
        props.putAll(policy.getCustomProperties());
        return props;
    }

    private static String getPluginDatsetType(final String type) throws BeaconException {
        ReplicationType replType = ReplicationHelper.getReplicationType(type);
        String pluginDatasetType;

        switch (replType) {
            case FS:
                pluginDatasetType = DataSet.DataSetType.HDFS.name();
                break;

            case HIVE:
                pluginDatasetType = DataSet.DataSetType.HIVE.name();
                break;

            default:
                throw new BeaconException("Job type {} is not supported", type);
        }
        return pluginDatasetType;
    }
}
