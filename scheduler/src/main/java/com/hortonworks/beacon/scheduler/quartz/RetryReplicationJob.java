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

import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Trigger;

/**
 * Replication job retry rescheduling.
 */
final class RetryReplicationJob {

    private static final BeaconLog LOG = BeaconLog.getLog(RetryReplicationJob.class);
    private static final String RETRY = "RETRY";

    private RetryReplicationJob() {
    }

    static void retry(Retry retry, JobExecutionContext context, JobContext jobContext) {
        BeaconLogUtils.setLogInfo(jobContext.getJobInstanceId());
        try {
            int instanceRunCount = StoreHelper.getInstanceRunCount(jobContext);
            if (instanceRunCount >= retry.getAttempts()) {
                LOG.info("All retry [{}] are exhausted.", instanceRunCount);
                context.getJobDetail().getJobDataMap().remove(QuartzDataMapEnum.IS_RETRY.getValue());
            } else {
                JobKey jobKey = context.getJobDetail().getKey();
                long delay = retry.getDelay() > 0 ? retry.getDelay() : Retry.RETRY_DELAY;
                Trigger trigger = QuartzTriggerBuilder.createTrigger(RandomStringUtils.randomAlphanumeric(32),
                        RETRY, jobKey, delay);
                trigger = trigger.getTriggerBuilder().usingJobData(QuartzDataMapEnum.RETRY_MARKER.getValue(),
                        BeaconConstants.SERVER_START_TIME).build();
                context.getScheduler().scheduleJob(trigger);
                LOG.info("Job is rescheduled for retry attempt: [{}] with delay: [{}s].", ++instanceRunCount, delay);
                context.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.IS_RETRY.getValue(), true);
            }
        } catch (Exception e) {
            LOG.error("Failed to reschedule retry of the job.", e);
            //TODO generate event.
        }
    }
}
