/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleanup retired policy, policy-instance and instance-job data from Store.
 */
public final class StoreCleanupService implements Callable<Void>, BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(StoreCleanupService.class);

    private static final StoreCleanupService INSTANCE = new StoreCleanupService();

    private Date cleanupDate;
    private int retiredOlderThan;

    private StoreCleanupService() {
    }

    public static StoreCleanupService get() {
        return INSTANCE;
    }

    @Override
    public Void call() throws Exception {
        cleanupDate = new Date(System.currentTimeMillis() - BeaconConstants.DAY_IN_MS * retiredOlderThan);
        try {
            LOG.info("StoreCleanupService execution started with cleanupDate: [{}].", DateUtil.formatDate(cleanupDate));
            RequestContext.get().startTransaction();
            cleanupInstanceJobs();
            cleanupPolicyInstances();
            cleanupPolicy();
            RequestContext.get().commitTransaction();
            LOG.info("StoreCleanupService execution completed successfully.");
            return null;
        } finally {
            RequestContext.get().rollbackTransaction();
            RequestContext.get().clear();
        }
    }

    private void cleanupPolicy() {
        PolicyPropertiesExecutor propsExecutor = new PolicyPropertiesExecutor();
        propsExecutor.deleteRetiredPolicyProps(cleanupDate);

        PolicyBean bean = new PolicyBean();
        bean.setRetirementTime(cleanupDate);
        PolicyExecutor executor = new PolicyExecutor(bean);
        executor.executeUpdate(PolicyQuery.DELETE_RETIRED_POLICY);
    }

    private void cleanupPolicyInstances() {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setRetirementTime(cleanupDate);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceQuery.DELETE_RETIRED_INSTANCE);
    }

    private void cleanupInstanceJobs() {
        InstanceJobBean bean = new InstanceJobBean();
        bean.setRetirementTime(cleanupDate);
        InstanceJobExecutor jobExecutor = new InstanceJobExecutor(bean);
        jobExecutor.executeUpdate(InstanceJobQuery.DELETE_RETIRED_JOBS);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    @Override
    public void init() throws BeaconException {
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
