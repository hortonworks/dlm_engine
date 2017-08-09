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
import com.hortonworks.beacon.scheduler.InstanceSchedulerDetail;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;

/**
 * Provides functionality for identifying parallel executing instance and record them as ignored instance.
 */
final class ParallelExecution {

    private static final BeaconLog LOG = BeaconLog.getLog(ParallelExecution.class);

    private ParallelExecution() {
    }

    static boolean checkParallelExecution(JobExecutionContext context) {
        // TODO check and prevent parallel execution execution of the job instance.
        // there is two cases:
        // 1. (covered) previous instance is still running and next instance triggered. (scheduler based, not store)
        // 2. (covered) After restart, previous instance is still in running state (store) but no actual jobs are
        // running.

        JobKey currentJob = context.getJobDetail().getKey();
        boolean parallel = isParallel(context);
        if (parallel) {
            SchedulerCache cache = SchedulerCache.get();
            InstanceSchedulerDetail detail = cache.getInstanceSchedulerDetail(currentJob.getName());
            String instanceId = detail != null ? detail.getInstanceId() : null;
            LOG.warn(MessageCode.SCHD_000032.name(), instanceId);
            context.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.PARALLEL_INSTANCE.getValue(),
                    instanceId);
        }
        return parallel;
    }

    // Parallel flag is set in the Trigger listener.
    private static boolean isParallel(JobExecutionContext context) {
        return context.getJobDetail().getJobDataMap().getBoolean(QuartzDataMapEnum.IS_PARALLEL.getValue());
    }
}
