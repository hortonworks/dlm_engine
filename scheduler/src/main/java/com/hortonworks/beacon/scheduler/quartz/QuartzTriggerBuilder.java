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

import com.hortonworks.beacon.util.DateUtil;
import org.quartz.JobKey;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

/**
 * Create different quartz triggers based on the start time and end time of the job.
 */
public final class QuartzTriggerBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzTriggerBuilder.class);

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
        LOG.info("Trigger [key: {}, start time: Now, end time: Never] is created.", policyId);
        return trigger;
    }

    private static Trigger createFixedEndTimeTrigger(String policyId, String group, Date endTime, int frequency) {
        if (endTime == null || endTime.before(new Date())) {
            throw new IllegalArgumentException("End time can not be null or earlier than current time.");
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
        LOG.info("Trigger [key: {}, start time: Now, end time: {}, frequency: {}] is created.", policyId, endTime,
                frequency);
        return trigger;
    }

    private static Trigger createFutureStartNeverEndingTrigger(String policyId,
                                                        String group, Date startTime, int frequency) {
        if (startTime == null || startTime.before(new Date())) {
            throw new IllegalArgumentException("Start time can not be null or earlier than current time.");
        }
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(policyId, group)
                .startAt(startTime)
                .withSchedule(simpleSchedule()
                        .withMisfireHandlingInstructionNowWithRemainingCount()
                        .withIntervalInSeconds(frequency)
                        .repeatForever())
                .build();
        LOG.info("Trigger [key: {}, start time: {}, end time: Never] is created.", policyId, startTime);
        return trigger;
    }

    private static Trigger createFutureStartEndTrigger(String policyId, String group,
                                                Date startTime, Date endTime, int frequency) {
        if (startTime == null || startTime.before(new Date())) {
            throw new IllegalArgumentException("Start time can not be null or earlier than current time.");
        }
        if (endTime == null || endTime.before(startTime)) {
            throw new IllegalArgumentException("End time can not be null or earlier than start time.");
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
        LOG.info("Trigger [key: {}, start time: {}, end time: {}] is created.", policyId, startTime, endTime);
        return trigger;
    }

    static Trigger createTrigger(String name, String group, JobKey jobkey, long fireDelay) {
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(name, group)
                .forJob(jobkey)
                .startAt(new Date(System.currentTimeMillis() + fireDelay * 1000))
                .build();
        LOG.info("Trigger key: [{}] for job: [{}] with fire time: {} is created.", trigger.getKey(),
                trigger.getJobKey(), DateUtil.formatDate(trigger.getStartTime()));
        return trigger;
    }

    public static Trigger createTrigger(String name, String group, int frequency, int repeatCount) {
        return TriggerBuilder.newTrigger()
                .withIdentity(name, group)
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(frequency)
                        .withRepeatCount(repeatCount)
                        .withMisfireHandlingInstructionNextWithExistingCount())
                .build();
    }
}
