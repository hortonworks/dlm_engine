/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.scheduler.InstanceSchedulerDetail;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.listeners.TriggerListenerSupport;

/**
 * Beacon extended implementation for TriggerListenerSupport.
 */
public class QuartzTriggerListener extends TriggerListenerSupport {

    private static final BeaconLog LOG = BeaconLog.getLog(QuartzTriggerListener.class);
    private String name;

    public QuartzTriggerListener(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void triggerFired(Trigger trigger, JobExecutionContext context) {
        JobKey jobKey = trigger.getJobKey();
        LOG.info(MessageCode.SCHD_000057.name(), trigger.getKey(), jobKey);
        SchedulerCache cache = SchedulerCache.get();
        synchronized (cache) {
            // Check the parallel for the START node only.
            if (BeaconQuartzScheduler.START_NODE_GROUP.equals(jobKey.getGroup())) {
                boolean exist = cache.exists(jobKey.getName());
                if (exist) {
                    LOG.info(MessageCode.SCHD_000058.name(), jobKey);
                    context.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.IS_PARALLEL.getValue(), true);
                } else {
                    cache.insert(jobKey.getName(), new InstanceSchedulerDetail());
                }
            }
        }
    }

    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
        boolean vetoTrigger = false;
        JobDataMap jobDataMap = trigger.getJobDataMap();
        if (jobDataMap.containsKey(QuartzDataMapEnum.RETRY_MARKER.getValue())) {
            long serverStartTime = jobDataMap.getLong(QuartzDataMapEnum.RETRY_MARKER.getValue());
            vetoTrigger = serverStartTime != BeaconConstants.SERVER_START_TIME;
            LOG.info(MessageCode.SCHD_000059.name(), vetoTrigger, trigger.getJobKey());
        }
        return vetoTrigger;
    }

    @Override
    public void triggerMisfired(Trigger trigger) {
        LOG.info(MessageCode.SCHD_000060.name(), trigger.getKey());
    }

    public void triggerComplete(Trigger trigger, JobExecutionContext context,
                                Trigger.CompletedExecutionInstruction triggerInstructionCode) {
        JobKey jobKey = context.getJobDetail().getKey();
        LOG.info(MessageCode.SCHD_000061.name(), trigger.getKey(), jobKey);
        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        boolean isEndJob = jobDataMap.getBoolean(QuartzDataMapEnum.IS_END_JOB.getValue());
        boolean isFailure = jobDataMap.getBoolean(QuartzDataMapEnum.IS_FAILURE.getValue());
        SchedulerCache cache = SchedulerCache.get();
        if (isEndJob || isFailure) {
            jobDataMap.remove(QuartzDataMapEnum.IS_FAILURE.getValue());
            cache.remove(jobKey.getName());
        }
    }
}
