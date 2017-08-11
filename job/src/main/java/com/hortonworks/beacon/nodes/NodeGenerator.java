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

import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.ReplicationType;

import java.util.ArrayList;
import java.util.List;

/**
 * Node generator for jobs.
 */
public final class NodeGenerator {

    public static final String START_NODE = "START-NODE";
    public static final String END_NODE = "END-NODE";

    private NodeGenerator() {
    }

    private static ReplicationJobDetails generateNode(String policyName, String identifier, String type) {
        return new ReplicationJobDetails(identifier, policyName, type, null);
    }

    public static List<ReplicationJobDetails> appendNodes(List<ReplicationJobDetails> details) {
        String policyName = details.get(0).getName();
        List<ReplicationJobDetails> jobDetails = new ArrayList<>(details);
        ReplicationJobDetails startNode = generateNode(policyName, START_NODE, ReplicationType.START.getName());
        jobDetails.add(0, startNode);
        ReplicationJobDetails endNode = generateNode(policyName, END_NODE, ReplicationType.END.getName());
        jobDetails.add(endNode);
        return jobDetails;
    }
}
