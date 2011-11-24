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

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.scheduler.BeaconScheduler;
import com.hortonworks.beacon.scheduler.InstanceSchedulerDetail;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreException;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.listeners.TriggerListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;

/**
 * Beacon extended implementation for TriggerListenerSupport.
 */
public class QuartzTriggerListener extends TriggerListenerSupport {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzTriggerListener.class);
    private String name;

    public QuartzTriggerListener(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void triggerFired(Trigger trigger, JobExecutionContext context) {
        RequestContext.setInitialValue();
        JobKey jobKey = trigger.getJobKey();
        String policyId = trigger.getJobKey().getName();
        BeaconLogUtils.prefixId(policyId);
        LOG.info("Trigger [key: {}] is fired for Job [key: {}]", trigger.getKey(), jobKey);
        try {
            StoreHelper.getPolicyById(policyId);
        } catch (NoSuchElementException e) {
            LOG.error("Policy [{}] not found. Removing policy trigger.", jobKey.getName(), e);
            trigger.getJobDataMap().put(QuartzDataMapEnum.POLICY_NOT_FOUND.getValue(), true);
            removeJob(trigger.getJobKey());
            return;
        } catch (BeaconStoreException e) {
            LOG.error("Exception while getting the policy.", e);
            return;
        }

        SchedulerCache cache = SchedulerCache.get();
        synchronized (cache) {
            // Check the parallel for the START node only.
            if (BeaconQuartzScheduler.START_NODE_GROUP.equals(jobKey.getGroup())) {
                boolean exist = cache.exists(jobKey.getName());
                if (exist) {
                    LOG.info("Setting the parallel flag for job: [{}]", jobKey);
                    context.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.IS_PARALLEL.getValue(), true);
                } else {
                    cache.insert(jobKey.getName(), new InstanceSchedulerDetail());
                }
            }
        }
    }

    private void removeJob(JobKey jobKey) {
        try {
            BeaconScheduler scheduler = Services.get().getService(BeaconQuartzScheduler.class);
            scheduler.deletePolicy(jobKey.getName());
        } catch (BeaconException e) {
            LOG.error("Exception while removing dangling jobs from quartz.", e);
        }
    }

    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
        boolean vetoTrigger = false;
        JobDataMap jobDataMap = trigger.getJobDataMap();
        if (jobDataMap.containsKey(QuartzDataMapEnum.RETRY_MARKER.getValue())) {
            long serverStartTime = jobDataMap.getLong(QuartzDataMapEnum.RETRY_MARKER.getValue());
            vetoTrigger = serverStartTime != BeaconConstants.SERVER_START_TIME;
        }
        vetoTrigger = vetoTrigger || jobDataMap.containsKey(QuartzDataMapEnum.POLICY_NOT_FOUND.getValue());
        LOG.debug("Veto trigger [{}] for job: [{}]", vetoTrigger, trigger.getJobKey());
        return vetoTrigger;
    }

    @Override
    public void triggerMisfired(Trigger trigger) {
        LOG.info("Trigger misfired for [key: {}].", trigger.getKey());
    }

    @Override
    public void triggerComplete(Trigger trigger, JobExecutionContext context,
                                Trigger.CompletedExecutionInstruction triggerInstructionCode) {
        try {
            JobKey jobKey = context.getJobDetail().getKey();
            LOG.info("Trigger [key: {}] completed for job [key: {}]", trigger.getKey(), jobKey);
            JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
            boolean isEndJob = jobDataMap.getBoolean(QuartzDataMapEnum.IS_END_JOB.getValue());
            boolean isFailure = jobDataMap.getBoolean(QuartzDataMapEnum.IS_FAILURE.getValue());
            SchedulerCache cache = SchedulerCache.get();
            if (isEndJob || isFailure) {
                jobDataMap.remove(QuartzDataMapEnum.IS_FAILURE.getValue());
                cache.remove(jobKey.getName());
            }
        } finally {
            RequestContext.get().clear();
        }
    }
}
