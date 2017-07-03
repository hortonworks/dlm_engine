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

package com.hortonworks.beacon.rb;

/**
 * Message ID map enum.
 */
public enum MessageCode {

    //Common Messages
    COMM_010001("Unable to parse retention age limit: {0}"),
    COMM_010002("Missing parameter: {0}"),
    COMM_010003("empty"),
    COMM_010004("success"),
    COMM_010005("Hive Action Type: {0} not supported:"),
    COMM_010006("mkdir failed for {0}"),
    COMM_010007("Invalid policy (Job) type: {0}"),
    COMM_010008("{0} cannot be null or empty"),
    COMM_010009("{0} type: {1} is not supported"),

    //Client module Messages
    CLIE_000001("Invalid entity type: {0}. Expected {1}."),
    CLIE_000002("Unable to initialize Beacon Client object. Cause {0}"),
    CLIE_000003("File not found:"),

    //Common module Messages
    COMM_000001("invalid data found while loading the context."),
    COMM_000002("The provided alias {0} cannot be resolved"),
    COMM_000003("The provided configuration cannot be resolved"),
    COMM_000004("Error creating credential entry using the credential provider"),
    COMM_000005("Invalid date format. Valid format: {0}"),
    COMM_000006("Unable to evaluate {0}"),
    COMM_000007("Function not found {0}: {1}"),
    COMM_000008("Invalid boundary {0}"),
    COMM_000009("Exception while getting Distributed FileSystem: {0}"),
    COMM_000010("Exception while getting FileSystem: {0}"),
    COMM_000011("Exception while getting FileSystem. Unable to check TGT for user {0}"),
    COMM_000012("Exception creating FileSystem: {0}"),
    COMM_000013("Cannot get valid scheme for {0}"),
    COMM_000014("Policy of Replication type ({0} is not supported)"),

    //Entity module Messages
    ENTI_000001("No remote beacon endpoint for HCFS policy: {0}"),
    ENTI_000002("{0} time cannot be earlier than {1} time."),
    ENTI_000003("Unhandled entity type: {0}"),
    ENTI_000004("Clusters {0} and {1} are not paired.Pair the clusters before submitting or scheduling the policy"),
    ENTI_000005("HCFS to HCFS replication is not allowed"),
    ENTI_000006("Either sourceCluster or targetCluster should be same as local cluster name: {0}"),
    ENTI_000007("Specified Replication frequency {0} seconds should not be less than {1} seconds"),

    //Job module Messages
    JOBS_000001("Invalid policy (Job) type: "),

    //Main module Messages
    MAIN_000001("Submit successful {0}: {1}"),
    MAIN_000002("Exception while sync delete policy to remote cluster: {0}."),
    MAIN_000003("Exception while obtain replication type:"),
    MAIN_000004("{0} command is already issued for {1}"),
    MAIN_000005("This operation is not allowed on source cluster: {0}.Try it on target cluster: {1}"),
    MAIN_000006("No jobs to schedule for: {0}"),
    MAIN_000007("{0} ({1}) is cannot be suspended. Current status: {2}"),
    MAIN_000008("{0} ({1}) suspended successfully"),
    MAIN_000009("{0} ({1}) is cannot be resumed. Current status: {2}"),
    MAIN_000010("{0} ({1}) suspended successfully"),
    MAIN_000011("Failed to delete policy from Beacon Scheduler name: {0}, type: {1}"),
    MAIN_000012("{0} ({1}) removed successfully."),
    MAIN_000013("remoteClusterName {0} cannot be same as localClusterName {1}. Cluster cannot be paired with itself"),
    MAIN_000014("Cluster {0} has already been paired with {1}"),
    MAIN_000015("For pairing both local {0} and remote cluster {1} should be submitted."),
    MAIN_000016("Clusters successfully paired"),
    MAIN_000017("Cluster {0} is not yet paired with {1}"),
    MAIN_000018("For unpairing both local {0} and remote cluster {1} should have been submitted and paired."),
    MAIN_000019("Active policies are present, unpair operation can not be done."),
    MAIN_000020("Submit and Sync policy successful ({0})"),
    MAIN_000021("Update status succeeded"),
    MAIN_000022("Event Name: {0} not supported"),
    MAIN_000023("Policy [{0}] is not in [RUNNING] state. Current status [{1}]"),
    MAIN_000024("policy instance abort status [{0}]"),
    MAIN_000025("Remote cluster {0} returned error: {1}"),
    MAIN_000026("Policy id should be present during sync."),
    MAIN_000027("{0} ({1}) scheduled successfully"),
    MAIN_000028("Policy [{0}] submitAndSchedule successful"),
    MAIN_000029("type={0}"),

    //Persistence module Messages
    PERS_000001("Cluster entity already exists with name: {0} version: {1}"),
    PERS_000002("Invalid named query parameter passed: {0}"),
    PERS_000003("Cluster entity does not exists name: {0}"),
    PERS_000004("Beacon data store is in inconsistent state. More than 1 result found."),
    PERS_000005("Invalid filter type provided. Input filter type: {0}"),
    PERS_000006("ClusterPair table is in inconsistent state. Number of records found: {0}"),
    PERS_000007("Policy already exists with name: {0}"),
    PERS_000008("Policy does not exists name: {0}"),
    PERS_000009("Beacon data store is in inconsistent state. More than 1 result found.Cluster name: {0}"),
    PERS_000010("Invalid filter key:value pair provided: {0}"),
    PERS_000011("Invalid filters provided: {0}"),
    PERS_000012("Parsing implementation is not present for filter: {0}"),

    //Plug-in module Messages
    PLUG_000001("Job type {0} not supported"),
    PLUG_000002("Plugin {0} not registered. Cannot perform the job"),
    PLUG_000003("Plugin {0} is in {1} and not in active state"),
    PLUG_000004("Job action type {0} not supported for plugin {1}"),
    PLUG_000005("plugin info cannot be null or empty. Registration failed"),
    PLUG_000006("No such plugin {0} has been registered with Beacon"),

    //Replication module Messages
    REPL_000001("No instance tracking info found for instance: {0}"),
    REPL_000002("Policy Type {0} not supported"),
    REPL_000003("Missing DR property for FS Replication: {0}"),
    REPL_000004("Exception occurred in init: {0}"),
    REPL_000005("Error occurred when checking target dir: {0} exists"),
    REPL_000006("Exception occurred while handling snapshot: {0}"),
    REPL_000007("Job exception occurred: {0}"),
    REPL_000008("Exception occurred while closing FileSystem: {0}"),
    REPL_000009("Error occurred when getting diff report for target dir: {0}, {1} fromSnapshot: {2}"
            + " & toSnapshot: {3}"),
    REPL_000010("Exception creating job client: {0}"),
    REPL_000011("isSnapShotsAvailable: {0} is not fully qualified path"),
    REPL_000012("Unable to verify if dir {0} is snapshot-able"),
    REPL_000013("{0} does not exist."),
    REPL_000014("Unable to find latest snapshot on targetDir {0}"),
    REPL_000015("Unable to create snapshot {0}"),
    REPL_000016("Unable to evict snapshots from dir {0}"),
    REPL_000017("Exception occurred while closing connection: {0}"),
    REPL_000018("Exception occurred initializing Hive Server: {0}"),
    REPL_000019("Interrupt occurred..."),
    REPL_000020("Missing DR property for Hive Replication: {0}"),

    //Scheduler module Messages
    SCHD_000001("No scheduled policy found."),
    SCHD_000002("Beacon scheduler configuration is not provided."),
    SCHD_000003("Key cannot have a null name!"),
    SCHD_000004("No suspended policy found"),
    SCHD_000005("{0} time can not be null or earlier than {1} time."),

    //Tools module Messages
    TOOL_000001("Schema {0} does not exists: {1}");

    private final String msg;

    MessageCode(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }
}
