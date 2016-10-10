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
import com.hortonworks.beacon.utils.SchedulerUtils;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

public class QuartzTriggerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzTriggerFactory.class);

    public Trigger createTrigger(Map<String, Object> dataMap, Date startTime, Date endTime, int frequencyInSec) {
        String triggerKey = SchedulerUtils.getUUID();
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey)
                .usingJobData(SchedulerUtils.prepareJobData(dataMap))
                .startAt(startTime)
                .endAt(endTime)
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(frequencyInSec))
                .build();
        LOG.info("Trigger [key: {}] is created.", triggerKey);
        return trigger;
    }

    public Trigger createTrigger(Date startTime, Date endTime, int frequencyInSec) {
        String triggerKey = SchedulerUtils.getUUID();
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey)
                .startAt(startTime)
                .endAt(endTime)
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(frequencyInSec))
                .build();
        LOG.info("Trigger [key: {}] is created.", triggerKey);
        return trigger;
    }

    public Trigger createTrigger(Date startTime, int repeatCount, int frequencyInSec) {
        String triggerKey = SchedulerUtils.getUUID();
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey)
                .startAt(startTime)
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(frequencyInSec)
                        .withRepeatCount(repeatCount))
                .build();
        LOG.info("Trigger [key: {}] is created.", triggerKey);
        return trigger;
    }

    public Trigger createTrigger(ReplicationJobDetails job) {
        String triggerKey = SchedulerUtils.getUUID();
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey)
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(job.getFrequency())
                        .withRepeatCount(0))
                .build();
        LOG.info("Trigger [key: {}] is created.", triggerKey);
        return trigger;
    }
}
