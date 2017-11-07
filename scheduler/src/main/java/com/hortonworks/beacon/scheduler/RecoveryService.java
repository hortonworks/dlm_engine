/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Beacon policy instance recovery service upon beacon server restart.
 */
public class RecoveryService implements BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryService.class);
    private static final String SERVICE_NAME = RecoveryService.class.getName();

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws BeaconException {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setStatus(JobStatus.RUNNING.name());
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        // Get the instances in running state.
        List<PolicyInstanceBean> instances = executor.executeSelectQuery(PolicyInstanceQuery.SELECT_INSTANCE_RUNNING);
        BeaconScheduler scheduler = ((SchedulerInitService)
                Services.get().getService(SchedulerInitService.SERVICE_NAME)).getScheduler();
        LOG.info("Number of instances for recovery: [{}]", instances.size());
        for (PolicyInstanceBean instance : instances) {
            // With current offset, find the respective job.
            String policyId = instance.getPolicyId();
            String offset = String.valueOf(instance.getCurrentOffset());
            String recoverInstance = instance.getInstanceId();
            // Trigger job with (policy id and offset)
            LOG.info("Recovering instanceId: [{}], current offset: [{}]", recoverInstance, offset);
            boolean recoveryStatus = scheduler.recoverPolicyInstance(policyId, offset, recoverInstance);
            if (!recoveryStatus) {
                handleRecoveryFailure(instance.getPolicyId(), instance.getInstanceId());
            }
            LOG.info("Recovered instanceId: [{}], request status: [{}]", recoverInstance, recoveryStatus);
        }
    }

    @Override
    public void destroy() throws BeaconException {
    }

    private void handleRecoveryFailure(String policyId, String instanceId) throws BeaconException {
        try {
            RequestContext.get().startTransaction();
            PolicyBean policyBean = new PolicyBean();
            policyBean.setId(policyId);
            PolicyExecutor executor = new PolicyExecutor(policyBean);
            policyBean = executor.getPolicy(PolicyQuery.GET_POLICY_BY_ID);
            if (policyBean.getStatus().equalsIgnoreCase(JobStatus.DELETED.name())) {
                markInstancesDeleted(instanceId, policyBean);
                markInstanceJobDeleted(instanceId, policyBean);
            }
            RequestContext.get().commitTransaction();
        } finally {
            RequestContext.get().rollbackTransaction();
            RequestContext.get().clear();
        }
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
        bean.setInstanceId(instanceId);
        bean.setStatus(policyBean.getStatus());
        bean.setRetirementTime(policyBean.getRetirementTime());
        PolicyInstanceExecutor instanceExecutor = new PolicyInstanceExecutor(bean);
        instanceExecutor.executeUpdate(PolicyInstanceQuery.UPDATE_INSTANCE_STATUS_RETIRE);
    }
}
