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

package com.hortonworks.beacon.scheduler.internal;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Scheduler;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.scheduler.HousekeepingScheduler;
import com.hortonworks.beacon.service.BeaconService;
import com.hortonworks.beacon.store.bean.InstanceJobBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor.InstanceJobQuery;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.store.executors.PolicyExecutor.PolicyQuery;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor.PolicyInstanceQuery;
import com.hortonworks.beacon.store.executors.PolicyPropertiesExecutor;
import com.hortonworks.beacon.util.DateUtil;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clean instance-jobs, policy-instances and policies in order for all retired policy from the store periodically.
 * Policy is marked retired when delete api is called.
 */
public final class StoreCleanupService implements Callable<Void>, BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(StoreCleanupService.class);

    private static final int BATCH_SIZE = 100;

    private static final StoreCleanupService INSTANCE = new StoreCleanupService();

    private Date cleanupDate;
    private int retiredOlderThan;

    private StoreCleanupService() {
    }

    public static StoreCleanupService get() {
        return INSTANCE;
    }

    @Override
    public Void call() {
        cleanupDate = new Date(System.currentTimeMillis() - BeaconConstants.DAY_IN_MS * retiredOlderThan);
        try {
            LOG.info("StoreCleanupService execution started with cleanupDate: [{}].", DateUtil.formatDate(cleanupDate));
            List<String> allPolicyIdsToBeArchived = getAllPolicyIdsToBeArchived();
            if (allPolicyIdsToBeArchived.size() == 0) {
                return null;
            }
            cleanupInstanceJobsPolicyInstancesAndPolicies(allPolicyIdsToBeArchived);
            LOG.info("StoreCleanupService execution completed successfully.");
            return null;
        } finally {
            RequestContext.get().rollbackTransaction();
            RequestContext.get().clear();
        }
    }

    private void cleanupInstanceJobsPolicyInstancesAndPolicies(List<String> allPolicyIdsToBeArchived) {
        for (String policyId :  allPolicyIdsToBeArchived) {
            while (true) {
                RequestContext.get().startTransaction();
                PolicyInstanceBean policyInstanceBean = new PolicyInstanceBean();
                policyInstanceBean.setPolicyId(policyId);
                PolicyInstanceExecutor policyInstanceExecutor = new PolicyInstanceExecutor(policyInstanceBean);
                List<String> instanceIds = policyInstanceExecutor
                        .getLimitedInstanceIds(PolicyInstanceQuery.GET_POLICY_INSTANCE_IDS, BATCH_SIZE);
                if (instanceIds.size() == 0) {
                    RequestContext.get().commitTransaction();
                    break;
                }
                cleanupInstanceJobs(instanceIds);
                cleanupPolicyInstances(instanceIds);
                RequestContext.get().commitTransaction();
            }
            cleanupPolicy(policyId);
        }
    }

    private List<String> getAllPolicyIdsToBeArchived() {
        PolicyBean bean = new PolicyBean();
        bean.setRetirementTime(cleanupDate);
        PolicyExecutor executor = new PolicyExecutor(bean);
        List<String> policiesToBeArchived = executor.getPoliciesToBeArchived(PolicyQuery.GET_POLICY_IDS_TO_BE_ARCHIVED);
        return policiesToBeArchived;
    }

    private void cleanupPolicy(String policyId) {
        RequestContext.get().startTransaction();
        PolicyPropertiesExecutor propsExecutor = new PolicyPropertiesExecutor();
        propsExecutor.deleteRetiredPolicyProps(policyId);
        PolicyExecutor policyExecutor = new PolicyExecutor(new PolicyBean());
        policyExecutor.executeBatchDelete(PolicyQuery.DELETE_RETIRED_POLICY, policyId);
        RequestContext.get().commitTransaction();
    }

    private void cleanupPolicyInstances(List<String> instanceIds) {
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(new PolicyInstanceBean());
        executor.executeBatchDelete(instanceIds, PolicyInstanceQuery.DELETE_POLICY_INSTANCE_BATCH);
    }

    private void cleanupInstanceJobs(List<String> instanceIds) {
        InstanceJobExecutor instanceJobExecutor = new InstanceJobExecutor(new InstanceJobBean());
        instanceJobExecutor.executeBatchDelete(instanceIds, InstanceJobQuery.DELETE_INSTANCE_JOB_BATCH);
    }

    @Override
    public void init() {
        Scheduler scheduler = BeaconConfig.getInstance().getScheduler();
        retiredOlderThan = scheduler.getRetiredPolicyOlderThan();
        int frequency = scheduler.getCleanupFrequency();
        int frequencyInMinute = frequency * 60; // Convert hours into minute.
        HousekeepingScheduler.schedule(this, frequencyInMinute, 10, TimeUnit.MINUTES);
    }

    @Override
    public void destroy() throws BeaconException {
        // Nothing to do.
    }
}
