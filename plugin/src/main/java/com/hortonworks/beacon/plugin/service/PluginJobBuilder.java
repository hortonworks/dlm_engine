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

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.ReplicationPolicyProperties;
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
        if (!Services.get().isRegistered(PluginManagerService.class.getName())) {
            return jobList;
        }

        if (!PluginManagerService.isPluginRegistered(PluginManagerService.DEFAULT_PLUGIN)) {
            // If ranger is not registered then no other plugin's are considered.
            LOG.info("Ranger plugin is not registered. Not adding any plugin jobs to add.");
            return jobList;
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
