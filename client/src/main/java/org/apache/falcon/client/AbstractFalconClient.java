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
package org.apache.falcon.client;

import org.apache.falcon.LifeCycle;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Abstract Client API to submit and manage Falcon Entities (Cluster, Feed, Process) jobs
 * against an Falcon instance.
 */
public abstract class AbstractFalconClient {

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck

    protected static final String FALCON_INSTANCE_ACTION_CLUSTERS = "falcon.instance.action.clusters";
    protected static final String FALCON_INSTANCE_SOURCE_CLUSTERS = "falcon.instance.source.clusters";

    /**
     * Submit a new entity. Entities can be of type feed, process or data end
     * points. Entity definitions are validated structurally against schema and
     * subsequently for other rules before they are admitted into the system.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param filePath Path for the entity definition
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult submit(String entityType, String filePath, String doAsUser) throws FalconCLIException,
            IOException;

    /**
     * Schedules an submitted process entity immediately.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param colo Cluster name.
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult schedule(EntityType entityType, String entityName, String colo, Boolean skipDryRun,
                                        String doAsuser, String properties) throws FalconCLIException;

    /**
     * Delete the specified entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param doAsUser Proxy User.
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult delete(EntityType entityType, String entityName,
                                     String doAsUser) throws FalconCLIException;

    /**
     * Validates the submitted entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param filePath Path for the entity definition to validate.
     * @param skipDryRun Dry run.
     * @param doAsUser Proxy User.
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult validate(String entityType, String filePath, Boolean skipDryRun,
                                       String doAsUser) throws FalconCLIException;

    /**
     * Updates the submitted entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param filePath Path for the entity definition to update.
     * @param skipDryRun Dry run.
     * @param doAsUser Proxy User.
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult update(String entityType, String entityName, String filePath,
                                                       Boolean skipDryRun, String doAsUser) throws FalconCLIException;

    /**
     * Get definition of the entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param doAsUser Proxy user.
     * @return
     * @throws FalconCLIException
     */
    public abstract Entity getDefinition(String entityType, String entityName,
                                         String doAsUser) throws FalconCLIException;



    /**
     *
     * @param type entity type
     * @param entity entity name
     * @param start start time
     * @param end end time
     * @param colo colo name
     * @param lifeCycles lifecycle of an entity (for ex : feed has replication,eviction).
     * @param filterBy filter operation can be applied to results
     * @param orderBy
     * @param sortOrder sort order can be asc or desc
     * @param offset offset while displaying results
     * @param numResults num of Results to output
     * @param doAsUser proxy user
     * @return
     * @throws FalconCLIException
     */
    public abstract InstancesResult getStatusOfInstances(String type, String entity,
                                                         String start, String end,
                                                         String colo, List<LifeCycle> lifeCycles, String filterBy,
                                                         String orderBy, String sortOrder,
                                                         Integer offset, Integer numResults,
                                                         String doAsUser) throws FalconCLIException;

    /**
     * Suspend an entity.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @return Status of the entity.
     * @throws FalconCLIException
     */
    public abstract APIResult suspend(EntityType entityType, String entityName, String colo, String doAsUser) throws
            FalconCLIException;

    /**
     * Resume a supended entity.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @return Result of the resume command.
     * @throws FalconCLIException
     */
    public abstract APIResult resume(EntityType entityType, String entityName, String colo, String doAsUser) throws
            FalconCLIException;

    /**
     * Get status of the entity.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @return Status of the entity.
     * @throws FalconCLIException
     */
    public abstract APIResult getStatus(EntityType entityType, String entityName, String colo, String doAsUser) throws
            FalconCLIException;

    /**
     * Kill currently running instance(s) of an entity.
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param start start time of the instance(s) that you want to refer to
     * @param end end time of the instance(s) that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @param doAsUser proxy user
     * @return Result of the kill operation.
     */
    public abstract InstancesResult killInstances(String type, String entity, String start, String end, String colo,
                                                  String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                                  String doAsUser) throws FalconCLIException,
            UnsupportedEncodingException;

    /**
     * Suspend instances of an entity.
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param start the start time of the instance(s) that you want to refer to
     * @param end the end time of the instance(s) that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @param doAsUser proxy user
     * @return Results of the suspend command.
     */
    public abstract InstancesResult suspendInstances(String type, String entity, String start, String end, String colo,
                                            String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                            String doAsUser) throws FalconCLIException, UnsupportedEncodingException;

    /**
     * Resume suspended instances of an entity.
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param start start time of the instance(s) that you want to refer to
     * @param end the end time of the instance(s) that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @param doAsUser proxy user
     * @return Results of the resume command.
     */
    public abstract InstancesResult resumeInstances(String type, String entity, String start, String end, String colo,
                                           String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                           String doAsUser) throws FalconCLIException, UnsupportedEncodingException;

    /**
     * Rerun instances of an entity. On issuing a rerun, by default the execution resumes from the last failed node in
     * the workflow.
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param start start is the start time of the instance that you want to refer to
     * @param end end is the end time of the instance that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @param isForced <optional param> can be used to forcefully rerun the entire instance.
     * @param doAsUser proxy user
     * @return Results of the rerun command.
     */
    public abstract InstancesResult rerunInstances(String type, String entity, String start, String end,
                                                   String filePath, String colo, String clusters,
                                                   String sourceClusters, List<LifeCycle> lifeCycles, Boolean isForced,
                                                   String doAsUser) throws FalconCLIException, IOException;

    /**
     * Get summary of instance/instances of an entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param start <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *                 By default, it is set to (end - (10 * entityFrequency)).
     * @param end <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *               Default is set to now.
     * @param colo <optional param> Colo on which the query should be run.
     * @param lifeCycles <optional param> Valid lifecycles for feed are Eviction/Replication(default) and for process
     *                   is Execution(default).
     * @param filterBy <optional param> Filter results by list of field:value pairs.
     *                 Example1: filterBy=STATUS:RUNNING,CLUSTER:primary-cluster
     *                 Example2: filterBy=Status:RUNNING,Status:KILLED
     *                 Supported filter fields are STATUS, CLUSTER.
     *                 Query will do an AND among filterBy fields.
     * @param orderBy <optional param> Field by which results should be ordered.
     *                Supports ordering by "cluster". Example: orderBy=cluster
     * @param sortOrder <optional param> Valid options are "asc" and "desc". Example: sortOrder=asc
     * @param doAsUser proxy user
     * @return Summary of the instances over the specified time range
     */
    public abstract InstancesSummaryResult getSummaryOfInstances(String type, String entity, String start, String end,
                                                                 String colo, List<LifeCycle> lifeCycles,
                                                                 String filterBy, String orderBy, String sortOrder,
                                                                 String doAsUser) throws FalconCLIException;

    /**
     * Get falcon feed instance availability.
     * @param type Valid options is feed.
     * @param entity Name of the entity.
     * @param start <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *              By default, it is set to (end - (10 * entityFrequency)).
     * @param end <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *            Default is set to now.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @return Feed instance availability status
     */
    public abstract FeedInstanceResult getFeedListing(String type, String entity, String start, String end, String colo,
                                                      String doAsUser) throws FalconCLIException;

    /**
     * Get log of a specific instance of an entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param start <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *                 By default, it is set to (end - (10 * entityFrequency)).
     * @param end <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *               Default is set to now.
     * @param colo <optional param> Colo on which the query should be run.
     * @param runId <optional param> Run Id.
     * @param lifeCycles <optional param> Valid lifecycles for feed are Eviction/Replication(default) and for process is
     *                   Execution(default).
     * @param filterBy <optional param> Filter results by list of field:value pairs.
     *                 Example: filterBy=STATUS:RUNNING,CLUSTER:primary-cluster
     *                 Supported filter fields are STATUS, CLUSTER, SOURCECLUSTER, STARTEDAFTER.
     *                 Query will do an AND among filterBy fields.
     * @param orderBy <optional param> Field by which results should be ordered.
     *                Supports ordering by "status","startTime","endTime","cluster".
     * @param sortOrder <optional param> Valid options are "asc" and "desc"
     * @param offset <optional param> Show results from the offset, used for pagination. Defaults to 0.
     * @param numResults <optional param> Number of results to show per request, used for pagination. Only integers > 0
     *                   are valid, Default is 10.
     * @param doAsUser proxy user
     * @return Log of specified instance.
     */
    public abstract InstancesResult getLogsOfInstances(String type, String entity, String start, String end,
                                                       String colo, String runId, List<LifeCycle> lifeCycles,
                                                       String filterBy, String orderBy, String sortOrder,
                                                       Integer offset, Integer numResults, String doAsUser) throws
            FalconCLIException;

    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    /**
     * Get the params passed to the workflow for an instance of feed/process.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param start should be the nominal time of the instance for which you want the params to be returned
     * @param colo <optional param> Colo on which the query should be run.
     * @param lifeCycles <optional param> Valid lifecycles for feed are Eviction/Replication(default) and for process is
     *                   Execution(default).
     * @param doAsUser proxy user
     * @return List of instances currently running.
     */
    public abstract InstancesResult getParamsOfInstance(String type, String entity, String start, String colo,
                                                        List<LifeCycle> lifeCycles, String doAsUser) throws
            FalconCLIException, UnsupportedEncodingException;

    /**
     * Get dependent instances for a particular instance.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity
     * @param instanceTime <mandatory param> time of the given instance
     * @param colo Colo on which the query should be run.
     * @return Dependent instances for the specified instance
     */
    public abstract InstanceDependencyResult getInstanceDependencies(String entityType, String entityName,
                                                                     String instanceTime, String colo) throws
            FalconCLIException;

    protected InputStream getServletInputStream(String clusters, String sourceClusters, String properties) throws
            FalconCLIException, UnsupportedEncodingException {

        InputStream stream;
        StringBuilder buffer = new StringBuilder();
        if (clusters != null) {
            buffer.append(FALCON_INSTANCE_ACTION_CLUSTERS).append('=').append(clusters).append('\n');
        }
        if (sourceClusters != null) {
            buffer.append(FALCON_INSTANCE_SOURCE_CLUSTERS).append('=').append(sourceClusters).append('\n');
        }
        if (properties != null) {
            buffer.append(properties);
        }
        stream = new ByteArrayInputStream(buffer.toString().getBytes());
        return (buffer.length() == 0) ? null : stream;
    }
}
