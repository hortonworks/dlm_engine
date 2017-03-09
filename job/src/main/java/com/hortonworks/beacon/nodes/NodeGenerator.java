/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
