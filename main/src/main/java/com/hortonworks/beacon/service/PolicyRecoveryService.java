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

package com.hortonworks.beacon.service;

import com.hortonworks.beacon.api.PolicyResource;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyDao;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.scheduler.HousekeepingScheduler;
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

    private static final Logger LOG = LoggerFactory.getLogger(PolicyRecoveryService.class);

    private BeaconQuartzScheduler scheduler;
    private PolicyDao policyDao;

    @Override
    public void init() throws BeaconException {
        scheduler = Services.get().getService(BeaconQuartzScheduler.class);
        policyDao = new PolicyDao();
        int frequency = BeaconConfig.getInstance().getScheduler().getPolicyCheckFrequency();
        HousekeepingScheduler.schedule(this, frequency, 0, TimeUnit.MINUTES);
    }

    @Override
    public void destroy() {
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
