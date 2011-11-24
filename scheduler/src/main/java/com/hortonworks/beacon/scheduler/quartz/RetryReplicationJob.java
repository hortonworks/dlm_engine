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

import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.job.JobContext;
import org.apache.commons.lang3.RandomStringUtils;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replication job retry rescheduling.
 */
final class RetryReplicationJob {

    private static final Logger LOG = LoggerFactory.getLogger(RetryReplicationJob.class);
    private static final String RETRY = "RETRY";

    private RetryReplicationJob() {
    }

    static void retry(Retry retry, JobExecutionContext context, JobContext jobContext) {
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
