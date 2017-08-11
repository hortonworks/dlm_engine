/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Helper class for scheduling housekeeping jobs.
 */
public final class HousekeepingScheduler {

    private static final BeaconLog LOG = BeaconLog.getLog(HousekeepingScheduler.class);

    private static int housekeepingThreads = BeaconConfig.getInstance().getScheduler().getHousekeepingThreads();
    private static ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(housekeepingThreads);

    private HousekeepingScheduler() {
    }

    public static void schedule(final Callable<Void> callable, int frequency, int initialDelay,
                                TimeUnit initialDelayUnit) {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    callable.call();
                } catch (Exception e) {
                    LOG.error(MessageCode.SCHD_000007.name(), callable.getClass().getName(), e);
                }
            }
        };
        scheduler.scheduleWithFixedDelay(runnable, initialDelay, frequency, initialDelayUnit);
    }
}
