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

package com.hortonworks.beacon.scheduler.internal;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Scheduler;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
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

/**
 * Cleanup retired policy, policy-instance and instance-job data from Store.
 */
public final class StoreCleanupService implements Callable<Void>, BeaconService {

    private static final BeaconLog LOG = BeaconLog.getLog(StoreCleanupService.class);

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
        LOG.info(MessageCode.SCHD_000024.name(), DateUtil.formatDate(cleanupDate));
        cleanupInstanceJobs();
        cleanupPolicyInstances();
        cleanupPolicy();
        LOG.info(MessageCode.SCHD_000025.name());
        return null;
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
