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
package com.hortonworks.beacon.scheduler.workflow;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.entity.util.PolicyDao;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.nodes.NodeGenerator;
import com.hortonworks.beacon.plugin.service.PluginJobBuilder;
import com.hortonworks.beacon.replication.JobBuilder;
import com.hortonworks.beacon.replication.PolicyJobBuilderFactory;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.dlmengine.BeaconReplicationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of Workflow interface.
 */
public final class BeaconWorkflow implements Workflow {

    private static final BeaconWorkflow INSTANCE = new BeaconWorkflow();
    private PolicyDao policyDao = new PolicyDao();
    private static final Logger LOG = LoggerFactory.getLogger(BeaconWorkflow.class);


    private BeaconWorkflow() {
    }

    public static BeaconWorkflow get() {
        return INSTANCE;
    }


    @Override
    public List<ReplicationJobDetails> createChainedWorkflow(BeaconReplicationPolicy policy) throws BeaconException {
        JobBuilder jobBuilder = PolicyJobBuilderFactory.getJobBuilder(policy);

        // final set of jobs.
        List<ReplicationJobDetails> jobs = new ArrayList<>();

        List<ReplicationJobDetails> policyJobs = jobBuilder.buildJob(policy);

        // Now get plugin related jobs and add it to front of the job list
        List<ReplicationJobDetails> pluginJobs = new PluginJobBuilder().buildJob(policy);

        if (pluginJobs != null && !pluginJobs.isEmpty()) {
            jobs.addAll(pluginJobs);
        }
        jobs.addAll(policyJobs);

        NodeGenerator.appendEndNode(jobs);

        // Update the policy jobs in policy table
        String jobList = getPolicyJobList(jobs);
        try {
            RequestContext.get().startTransaction();
            policyDao.updatePolicyJobs(policy.getPolicyId(), policy.getName(), jobList);
            RequestContext.get().commitTransaction();
        } finally {
            RequestContext.get().rollbackTransaction();
        }
        LOG.info("Jobs for this run: {}", jobList);

        return jobs;
    }

    private static String getPolicyJobList(final List<ReplicationJobDetails> jobs) {
        StringBuilder jobList = new StringBuilder();
        for (ReplicationJobDetails job : jobs) {
            if (jobList.length() > 0) {
                jobList.append(",");
            }
            jobList.append(job.getIdentifier());
        }
        return jobList.toString();
    }
}
