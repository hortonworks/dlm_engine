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

/**
 * Quartz data map enum.
 */
public enum QuartzDataMapEnum {
    DETAILS("details"),
    COUNTER("counter"),
    CHAINED("chained"),
    NO_OF_JOBS("no_of_jobs"),
    JOB_CONTEXT("job_context"),
    IS_PARALLEL("is_parallel"),
    PARALLEL_INSTANCE("parallel_instance"),
    ADMIN_JOB("admin_job"),
    IS_END_JOB("is_end_job"),
    IS_FAILURE("is_failure"),
    IS_RETRY("is_retry"),
    IS_RECOVERY("is_recovery"),
    RECOVER_INSTANCE("recover_instance"),
    RETRY_MARKER("retry_marker"),
    POLICY_NOT_FOUND("policy_not_found");

    private final String value;

    QuartzDataMapEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
