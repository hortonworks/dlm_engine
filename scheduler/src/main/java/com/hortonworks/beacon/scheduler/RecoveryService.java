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

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.service.BeaconService;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.bean.InstanceJobBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor.InstanceJobQuery;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.store.executors.PolicyExecutor.PolicyQuery;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor.PolicyInstanceQuery;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Beacon policy instance recovery service upon beacon server restart.
 */
public class RecoveryService implements BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryService.class);

    private static Queue<String> queue = new ConcurrentLinkedQueue<>();

    public static boolean addToRecovery(String instanceId) {
        LOG.info("Instance [{}] added to recovery queue", instanceId);
        return queue.add(instanceId);
    }

    @Override
    public void init() throws BeaconException {
        RequestContext.setInitialValue();
        try {
            PolicyInstanceBean bean = new PolicyInstanceBean();
            bean.setStatus(JobStatus.RUNNING.name());
            PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
            // Get the instances in running state.
            List<PolicyInstanceBean> instances =
                    executor.executeSelectQuery(PolicyInstanceQuery.SELECT_INSTANCE_RUNNING);
            BeaconScheduler scheduler = Services.get().getService(BeaconQuartzScheduler.class);
            LOG.info("Number of instances for recovery: [{}]", instances.size());
            for (PolicyInstanceBean instance : instances) {
                // With current offset, find the respective job.
                String policyId = instance.getPolicyId();
                String offset = String.valueOf(instance.getCurrentOffset());
                String recoverInstance = instance.getInstanceId();
                // Trigger job with (policy id and offset)
                LOG.info("Recovering instanceId: [{}], current offset: [{}]", recoverInstance, offset);
                boolean recoveryStatus = scheduler.recoverPolicyInstance(policyId, offset, recoverInstance);
                LOG.info("Recovered instanceId: [{}], recovery status: [{}]", recoverInstance, recoveryStatus);
                if (!recoveryStatus) {
                    handleRecoveryFailure(instance.getPolicyId(), instance.getInstanceId());
                }
            }
        } finally {
            RequestContext.get().rollbackTransaction();
            RequestContext.get().clear();
        }
        attachInternalInstanceFailureListener();
    }

    private void attachInternalInstanceFailureListener() {
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
        timer.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                while (!queue.isEmpty()) {
                    String instanceId = StringUtils.EMPTY;
                    try {
                        instanceId = queue.poll();
                        markInstanceStatus(instanceId, "Instance internal failure.", JobStatus.FAILED);
                    } catch (Throwable t) {
                        LOG.info("Failure in marking the instance {} as killed.", instanceId);
                        queue.add(instanceId);
                        try {
                            Thread.sleep(15000);
                        } catch (InterruptedException e1) {
                            LOG.error("Interrupted", e1);
                        }
                    }
                }
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    @Override
    public void destroy() throws BeaconException {
    }

    private void handleRecoveryFailure(String policyId, String instanceId) throws BeaconException {
        RequestContext.get().startTransaction();
        PolicyBean policyBean = new PolicyBean();
        policyBean.setId(policyId);
        PolicyExecutor executor = new PolicyExecutor(policyBean);
        policyBean = executor.getPolicy(PolicyQuery.GET_POLICY_BY_ID);
        if (policyBean.getStatus().equalsIgnoreCase(JobStatus.DELETED.name())) {
            LOG.info("Marking instanceId: [{}] as deleted", instanceId);
            markInstancesDeleted(instanceId, policyBean);
            markInstanceJobDeleted(instanceId, policyBean);
        } else {
            LOG.info("Marking instanceId: [{}] as killed", instanceId);
            markInstanceStatus(instanceId, "Another instance got triggered before recovery", JobStatus.KILLED);
        }
        RequestContext.get().commitTransaction();
    }

    private static void markInstanceJobDeleted(String instanceId, PolicyBean policyBean) {
        InstanceJobBean jobBean = new InstanceJobBean();
        jobBean.setInstanceId(instanceId);
        jobBean.setRetirementTime(policyBean.getRetirementTime());
        InstanceJobExecutor jobExecutor = new InstanceJobExecutor(jobBean);
        jobExecutor.executeUpdate(InstanceJobQuery.DELETE_INSTANCE_JOB);
    }

    private static void markInstancesDeleted(String instanceId, PolicyBean policyBean) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setEndTime(new Date());
        bean.setInstanceId(instanceId);
        bean.setStatus(policyBean.getStatus());
        bean.setRetirementTime(policyBean.getRetirementTime());
        PolicyInstanceExecutor instanceExecutor = new PolicyInstanceExecutor(bean);
        instanceExecutor.executeUpdate(PolicyInstanceQuery.UPDATE_INSTANCE_STATUS_RETIRE);
    }

    private static void markInstanceStatus(String instanceId, String message, JobStatus jobStatus) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setInstanceId(instanceId);
        bean.setEndTime(new Date());
        bean.setRetirementTime(new Date());
        bean.setStatus(jobStatus.name());
        bean.setMessage(message);
        PolicyInstanceExecutor instanceExecutor = new PolicyInstanceExecutor(bean);
        instanceExecutor.executeUpdate(PolicyInstanceQuery.UPDATE_INSTANCE_COMPLETE);
    }
}
