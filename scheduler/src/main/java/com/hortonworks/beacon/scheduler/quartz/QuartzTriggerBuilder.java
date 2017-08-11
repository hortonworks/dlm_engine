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

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.util.DateUtil;
import org.quartz.JobKey;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import java.util.Date;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

/**
 * Create different quartz triggers based on the start time and end time of the job.
 */
public final class QuartzTriggerBuilder {

    private static final BeaconLog LOG = BeaconLog.getLog(QuartzTriggerBuilder.class);

    private QuartzTriggerBuilder() {
    }

    public static Trigger createTrigger(String policyId, String group, Date startTime, Date endTime, int frequency) {

        if (startTime != null && endTime != null) {
            return createFutureStartEndTrigger(policyId, group, startTime, endTime, frequency);
        } else if (startTime == null && endTime != null) {
            return createFixedEndTimeTrigger(policyId, group, endTime, frequency);
        } else if (startTime != null && endTime == null) {
            return createFutureStartNeverEndingTrigger(policyId, group, startTime, frequency);
        } else {
            return createNeverEndingTrigger(policyId, group, frequency);
        }
    }

    private static Trigger createNeverEndingTrigger(String policyId, String group, int frequency) {
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(policyId, group)
                .startNow()
                .withSchedule(simpleSchedule()
                        .withMisfireHandlingInstructionNowWithRemainingCount()
                        .withIntervalInSeconds(frequency)
                        .repeatForever())
                .build();
        LOG.info(MessageCode.SCHD_000054.name(), policyId, "Now", "Never");
        return trigger;
    }

    private static Trigger createFixedEndTimeTrigger(String policyId, String group, Date endTime, int frequency) {
        if (endTime == null || endTime.before(new Date())) {
            throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.SCHD_000005.name(), "End", "current"));
        }
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(policyId, group)
                .startNow()
                .endAt(endTime)
                .withSchedule(simpleSchedule()
                        .withMisfireHandlingInstructionNowWithRemainingCount()
                        .withIntervalInSeconds(frequency)
                        .repeatForever())
                .build();
        LOG.info(MessageCode.SCHD_000055.name(), policyId, "Now", endTime,
                frequency);
        return trigger;
    }

    private static Trigger createFutureStartNeverEndingTrigger(String policyId,
                                                        String group, Date startTime, int frequency) {
        if (startTime == null || startTime.before(new Date())) {
            throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.SCHD_000005.name(), "Start", "current"));
        }
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(policyId, group)
                .startAt(startTime)
                .withSchedule(simpleSchedule()
                        .withMisfireHandlingInstructionNowWithRemainingCount()
                        .withIntervalInSeconds(frequency)
                        .repeatForever())
                .build();
        LOG.info(MessageCode.SCHD_000054.name(), policyId, startTime, "Never");
        return trigger;
    }

    private static Trigger createFutureStartEndTrigger(String policyId, String group,
                                                Date startTime, Date endTime, int frequency) {
        if (startTime == null || startTime.before(new Date())) {
            throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.SCHD_000005.name(), "Start", "current"));
        }
        if (endTime == null || endTime.before(startTime)) {
            throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.SCHD_000005.name(), "End", "start"));
        }

        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(policyId, group)
                .startAt(startTime)
                .endAt(endTime)
                .withSchedule(simpleSchedule()
                        .withMisfireHandlingInstructionNowWithRemainingCount()
                        .withIntervalInSeconds(frequency)
                        .repeatForever())
                .build();
        LOG.info(MessageCode.SCHD_000054.name(), policyId, startTime, endTime);
        return trigger;
    }

    static Trigger createTrigger(String name, String group, JobKey jobkey, long fireDelay) {
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(name, group)
                .forJob(jobkey)
                .startAt(new Date(System.currentTimeMillis() + fireDelay * 1000))
                .build();
        LOG.info(MessageCode.SCHD_000056.name(), trigger.getKey(),
                trigger.getJobKey(), DateUtil.formatDate(trigger.getStartTime()));
        return trigger;
    }
}
