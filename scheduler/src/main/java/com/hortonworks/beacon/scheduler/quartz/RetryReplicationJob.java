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

import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.rb.MessageCode;

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
                LOG.info(MessageCode.SCHD_000062.name(), instanceRunCount);
                context.getJobDetail().getJobDataMap().remove(QuartzDataMapEnum.IS_RETRY.getValue());
            } else {
                JobKey jobKey = context.getJobDetail().getKey();
                long delay = retry.getDelay() > 0 ? retry.getDelay() : Retry.RETRY_DELAY;
                Trigger trigger = QuartzTriggerBuilder.createTrigger(RandomStringUtils.randomAlphanumeric(32),
                        RETRY, jobKey, delay);
                trigger = trigger.getTriggerBuilder().usingJobData(QuartzDataMapEnum.RETRY_MARKER.getValue(),
                        BeaconConstants.SERVER_START_TIME).build();
                context.getScheduler().scheduleJob(trigger);
                LOG.info(MessageCode.SCHD_000063.name(), ++instanceRunCount, delay);
                context.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.IS_RETRY.getValue(), true);
            }
        } catch (Exception e) {
            LOG.error(MessageCode.SCHD_000064.name(), e);
            //TODO generate event.
        }
    }
}
