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

/**
 * Used for holding the information about the executing instance.
 *
 * e.g. Is there any interrupt, which needs to be checked and handled in
 * {@link com.hortonworks.beacon.scheduler.quartz.QuartzJob}
 */
public class InstanceSchedulerDetail {

    private boolean interrupt;

    public boolean isInterrupt() {
        return interrupt;
    }

    public void setInterrupt(boolean interrupt) {
        this.interrupt = interrupt;
    }
}
