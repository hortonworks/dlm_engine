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

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.replication.ReplicationJobDetails;
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
public class QuartzTriggerBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzTriggerBuilder.class);

    public Trigger createTrigger(ReplicationJobDetails job, String policyId,
                                 Date startTime, Date endTime, int frequency) {

        if (startTime != null && endTime != null) {
            return createFutureStartEndTrigger(job, policyId, startTime, endTime, frequency);
        } else if (startTime == null && endTime != null) {
            return createFixedEndTimeTrigger(job, policyId, endTime, frequency);
        } else if (startTime != null && endTime == null) {
            return createFutureStartNeverEndingTrigger(job, policyId, startTime, frequency);
        } else {
            return createNeverEndingTrigger(job, policyId, frequency);
        }
    }

    public Trigger createSingleInstanceTrigger(ReplicationJobDetails job, String policyId, int frequency) {
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(policyId, job.getIdentifier())
                .startNow()
                .withSchedule(simpleSchedule()
                        .withMisfireHandlingInstructionNowWithRemainingCount()
                        .withIntervalInSeconds(frequency)
                        .withRepeatCount(0))
                .build();
        LOG.info("Single instance trigger [key: {}] is created.", policyId);
        return trigger;
    }

    private Trigger createNeverEndingTrigger(ReplicationJobDetails job, String policyId, int frequency) {
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(policyId, job.getIdentifier())
                .startNow()
                .withSchedule(simpleSchedule()
                        .withMisfireHandlingInstructionNowWithRemainingCount()
                        .withIntervalInSeconds(frequency)
                        .repeatForever())
                .build();
        LOG.info("Trigger [key: {}, StartTime: {}, EndTime: {}] is created.", policyId, "Now", "Never");
        return trigger;
    }

    private Trigger createFixedEndTimeTrigger(ReplicationJobDetails job, String policyId, Date endTime, int frequency) {
        if (endTime == null || endTime.before(new Date())) {
            throw new IllegalArgumentException("End time can not be null or earlier than current time.");
        }
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(policyId, job.getIdentifier())
                .startNow()
                .endAt(endTime)
                .withSchedule(simpleSchedule()
                        .withMisfireHandlingInstructionNowWithRemainingCount()
                        .withIntervalInSeconds(frequency)
                        .repeatForever())
                .build();
        LOG.info("Trigger [key: {}, StartTime: {}, EndTime: {}] is created.", policyId, "Now", endTime);
        return trigger;
    }

    private Trigger createFutureStartNeverEndingTrigger(ReplicationJobDetails job, String policyId,
                                                        Date startTime, int frequency) {
        if (startTime == null || startTime.before(new Date())) {
            throw new IllegalArgumentException("Start time can not be null or earlier than current time.");
        }
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(policyId, job.getIdentifier())
                .startAt(startTime)
                .withSchedule(simpleSchedule()
                        .withMisfireHandlingInstructionNowWithRemainingCount()
                        .withIntervalInSeconds(frequency)
                        .repeatForever())
                .build();
        LOG.info("Trigger [key: {}, StartTime: {}, EndTime: {}] is created.", policyId, startTime, "Never");
        return trigger;
    }

    private Trigger createFutureStartEndTrigger(ReplicationJobDetails job,
                                                String policyId, Date startTime, Date endTime, int frequency) {
        if (startTime == null || startTime.before(new Date())) {
            throw new IllegalArgumentException("Start time can not be null or earlier than current time.");
        }
        if (endTime == null || endTime.before(startTime)) {
            throw new IllegalArgumentException("End time can not be null or earlier than start time.");
        }

        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(policyId, job.getIdentifier())
                .startAt(startTime)
                .endAt(endTime)
                .withSchedule(simpleSchedule()
                        .withMisfireHandlingInstructionNowWithRemainingCount()
                        .withIntervalInSeconds(frequency)
                        .repeatForever())
                .build();
        LOG.info("Trigger [key: {}, StartTime: {}, EndTime: {}] is created.", policyId, startTime, endTime);
        return trigger;
    }
}
