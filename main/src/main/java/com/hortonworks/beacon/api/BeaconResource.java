/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.UserPrivilegesResult;
import com.hortonworks.beacon.client.result.DBListResult;
import com.hortonworks.beacon.client.result.FileListResult;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLogUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Beacon resource management operations as REST API. Root resource (exposed at "myresource" path).
 */
@Path("/api/beacon")
public class BeaconResource extends AbstractResourceManager {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconResource.class);

    @GET
    @Path("file/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public FileListResult listFiles(@QueryParam("path") String path,
                                    @QueryParam("credId") String cloudCredId) {
        try {
            if (StringUtils.isBlank(path)) {
                throw BeaconWebException.newAPIException("FS Path can't be empty");
            }
            if (StringUtils.isBlank(cloudCredId)) {
                LOG.info("List FS path {} details on cluster {}", path, ClusterHelper.getLocalCluster().getName());
                return listFiles(ClusterHelper.getLocalCluster(), path);
            } else {
                LOG.info("List Cloud path {} details on cred id {}", path, cloudCredId);
                return listCloudFiles(path, cloudCredId);
            }
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("hive/listDBs")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public DBListResult listHiveDBs() {
        try {
            LOG.info("List Database with tables on cluster {}", ClusterHelper.getLocalCluster().getName());
            return listHiveDBs(ClusterHelper.getLocalCluster());
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("hive/listTables")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public DBListResult listHiveTables(@QueryParam("db") String dbName) {
        try {
            if (StringUtils.isBlank(dbName)) {
                throw BeaconWebException.newAPIException("Database name can't be empty");
            }

            LOG.info("List Database with tables on cluster {}", ClusterHelper.getLocalCluster().getName());
            return listHiveTables(ClusterHelper.getLocalCluster(), dbName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("user")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public UserPrivilegesResult getUserPrivileges() {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
                conf.get("dfs.namenode.kerberos.principal"));
        try {
            //Get groups for the API user using namenode's getGroupsForUser RPC
            String userName = RequestContext.get().getUser();
            RPC.setProtocolEngine(conf, GetUserMappingsProtocolPB.class, ProtobufRpcEngine.class);
            GetUserMappingsProtocolPB rpcProxy = RPC.getProxy(GetUserMappingsProtocolPB.class,
                    RPC.getProtocolVersion(GetUserMappingsProtocolPB.class),
                    conf.getSocketAddr("dfs.namenode.rpc-address", "localhost", 9820), conf);
            GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto request =
                    GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto
                            .newBuilder().setUser(userName).build();
            GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto response =
                    rpcProxy.getGroupsForUser(null, request);
            List<String> userGroups = response.getGroupsList();
            LOG.debug("Groups for user {}: {}", userName, StringUtils.join(userGroups, ", "));

            //Get dfs.permissions.superusergroup from hadoop conf
            String superUserGroups = conf.getTrimmed("dfs.permissions.superusergroup");
            LOG.debug("Super user groups for hdfs(dfs.permissions.superusergroup): {}", superUserGroups);
            List<String> superUserGroupsList = Arrays.asList(superUserGroups.split("\\s*,\\s*"));

            //Intersection of user groups and super user groups implies that the API user is HDFS superuser
            Collection intersection = CollectionUtils.intersection(userGroups, superUserGroupsList);
            UserPrivilegesResult result = new UserPrivilegesResult();
            result.setUserName(userName);
            result.setHdfsSuperUser(!intersection.isEmpty());
            return result;
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e);
        }
    }

    @GET
    @Path("instance/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyInstanceList listInstances(@QueryParam("filterBy") String filters,
                                            @DefaultValue("startTime") @QueryParam("orderBy") String orderBy,
                                            @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                            @DefaultValue("0") @QueryParam("offset") Integer offset,
                                            @QueryParam("numResults") Integer resultsPerPage,
                                            @DefaultValue("false") @QueryParam("archived") String archived) {
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        try {
            boolean isArchived = Boolean.parseBoolean(archived);
            return listInstance(filters, orderBy, sortBy, offset, resultsPerPage, isArchived);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("logs")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult getPolicyLogs(@QueryParam("filterBy") String filters,
                                   @QueryParam("start") String startStr,
                                   @QueryParam("end") String endStr,
                                   @DefaultValue("12") @QueryParam("frequency") Integer frequency,
                                   @DefaultValue("100") @QueryParam("numResults") Integer numLogs) {
        try {
            if (StringUtils.isBlank(filters)) {
                BeaconLogUtils.deletePrefix();
                throw BeaconWebException.newAPIException("Query param [filterBy] cannot be null or empty");
            }
            return getPolicyLogsInternal(filters, startStr, endStr, frequency, numLogs);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        } finally{
            BeaconLogUtils.deletePrefix();
        }
    }

    private FileListResult listFiles(Cluster cluster, String path) throws BeaconException {
        try {
            return datasetListing.listFiles(cluster, path);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private FileListResult listCloudFiles(String path, String cloudCredId) {
        try {
            path = prepareCloudPath(path, cloudCredId);
            Configuration configuration = cloudConf(cloudCredId);
            return datasetListing.listCloudFiles(configuration, path);
        } catch (URISyntaxException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    private DBListResult listHiveDBs(Cluster cluster) throws BeaconException {
        try {
            return datasetListing.listHiveDBDetails(cluster, " ");
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private DBListResult listHiveTables(Cluster cluster, String dbName) throws BeaconException {
        try {
            return datasetListing.listHiveDBDetails(cluster, dbName);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private APIResult getPolicyLogsInternal(String filters, String startStr, String endStr,
                                            int frequency, int numLogs) throws BeaconException {
        try {
            String logString = logRetrieval.getPolicyLogs(filters, startStr, endStr, frequency, numLogs);
            return new APIResult(APIResult.Status.SUCCEEDED, logString);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }
}
