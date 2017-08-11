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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.service.BeaconService;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor.PolicyInstanceQuery;

import java.util.List;

/**
 * Beacon policy instance recovery service upon beacon server restart.
 */
public class RecoveryService implements BeaconService {

    private static final BeaconLog LOG = BeaconLog.getLog(RecoveryService.class);
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
        LOG.info(MessageCode.SCHD_000008.name(), instances.size());
        for (PolicyInstanceBean instance : instances) {
            // With current offset, find the respective job.
            String policyId = instance.getPolicyId();
            String offset = String.valueOf(instance.getCurrentOffset());
            String recoverInstance = instance.getInstanceId();
            // Trigger job with (policy id and offset)
            LOG.info(MessageCode.SCHD_000009.name(), recoverInstance, offset);
            boolean recoveryStatus = scheduler.recoverPolicyInstance(policyId, offset, recoverInstance);
            LOG.info(MessageCode.SCHD_000010.name(), recoverInstance, recoveryStatus);
        }
    }

    @Override
    public void destroy() throws BeaconException {
    }
}
