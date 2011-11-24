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

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.config.BeaconConfig;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for scheduling housekeeping jobs.
 */
public final class HousekeepingScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(HousekeepingScheduler.class);

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
                    RequestContext.setInitialValue();
                    callable.call();
                } catch (Exception e) {
                    LOG.error("Exception while execution {}", callable.getClass().getName(), e);
                } finally {
                    RequestContext.get().clear();
                }
            }
        };
        scheduler.scheduleWithFixedDelay(runnable, initialDelay, frequency, initialDelayUnit);
    }
}
