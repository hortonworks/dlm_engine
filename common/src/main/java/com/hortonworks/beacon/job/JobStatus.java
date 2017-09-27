/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.job;

import java.util.Arrays;
import java.util.List;

/**
 * Status for Beacon policy and policy instances.
 */
public enum JobStatus {
    RUNNING,
    FAILED,
    SUCCESS,
    SUBMITTED,
    DELETED,
    SUSPENDED,
    KILLED,
    SKIPPED,

    // Final status for policy
    SUCCEEDED,
    SUCCEEDEDWITHSKIPPED,
    FAILEDWITHSKIPPED;


    public static List<String> getCompletionStatus() {
        return Arrays.asList(
                JobStatus.SUCCEEDED.name(),
                JobStatus.FAILED.name(),
                JobStatus.SUCCEEDEDWITHSKIPPED.name(),
                JobStatus.FAILEDWITHSKIPPED.name()
        );
    }
}
