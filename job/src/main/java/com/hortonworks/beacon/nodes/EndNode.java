/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.nodes;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;

/**
 * End node implementation.
 */
public class EndNode extends InstanceReplication implements BeaconJob {

    private static final BeaconLog LOG = BeaconLog.getLog(EndNode.class);

    public EndNode(ReplicationJobDetails jobDetails) {
        super(jobDetails);
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {

    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        LOG.info(MessageCode.JOBS_000001.name(),
                jobContext.getJobInstanceId(), getDetails().getType());
        setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS);
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {

    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        LOG.info(MessageCode.COMM_010012.name(), jobContext.getJobInstanceId());
    }
}
