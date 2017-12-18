/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.service;

import com.hortonworks.beacon.api.PolicyResource;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyDao;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.scheduler.HousekeepingScheduler;
import com.hortonworks.beacon.scheduler.SchedulerInitService;
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.store.executors.PolicyExecutor.PolicyQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Check submitted polices and schedule them into scheduler.
 */
public class PolicyRecoveryService implements Callable<Void>, BeaconService {

    public static final String SERVICE_NAME = PolicyRecoveryService.class.getName();
    private static final Logger LOG = LoggerFactory.getLogger(PolicyRecoveryService.class);

    private BeaconQuartzScheduler scheduler;
    private PolicyDao policyDao;

    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws BeaconException {
        SchedulerInitService schedulerService = Services.get().getService(SchedulerInitService.SERVICE_NAME);
        scheduler = schedulerService.getScheduler();
        policyDao = new PolicyDao();
        int frequency = BeaconConfig.getInstance().getScheduler().getPolicyCheckFrequency();
        HousekeepingScheduler.schedule(this, frequency, 0, TimeUnit.MINUTES);
    }

    @Override
    public void destroy() throws BeaconException {
        scheduler = null;
    }

    @Override
    public Void call() throws Exception {
        policyRecovery();
        return null;
    }

    private void policyRecovery() throws BeaconException {
        PolicyExecutor executor = new PolicyExecutor(new PolicyBean());
        List<PolicyBean> policies = executor.getPolicies(PolicyQuery.GET_POLICY_RECOVERY);
        if (policies != null && policies.size() != 0) {
            LOG.info("policies [{}] found for recovery with status [SUBMITTED].", policies.size());
            schedulePolicies(policies);
        }
    }

    private void schedulePolicies(List<PolicyBean> schedulablePolices) {
        PolicyResource policyResource = new PolicyResource();
        for (PolicyBean bean : schedulablePolices) {
            try {
                if (ClusterHelper.isLocalCluster(bean.getTargetCluster())
                        && !scheduler.checkExists(bean.getId(), BeaconQuartzScheduler.START_NODE_GROUP)) {
                    ReplicationPolicy policy = policyDao.getReplicationPolicy(bean);
                    LOG.info("Scheduling policy: [{}] with startTime: [{}].", policy.getName(), policy.getStartTime());
                    policyResource.scheduleInternal(policy);
                }
            } catch (Exception e) {
                LOG.error("Exception while recovery scheduling the policy [{}].", bean.getId(), e);
            }
        }
    }
}
