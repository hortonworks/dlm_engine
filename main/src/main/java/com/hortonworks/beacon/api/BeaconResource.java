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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.SchemeType;
import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.UserPrivilegesResult;
import com.hortonworks.beacon.client.result.DBListResult;
import com.hortonworks.beacon.client.result.FileListResult;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import static com.hortonworks.beacon.util.FSUtils.merge;

/**
 * Beacon resource management operations as REST API. Root resource (exposed at "myresource" path).
 */
@Path("/api/beacon")
public class BeaconResource extends AbstractResourceManager {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconResource.class);

    @GET
    @Path("file/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public FileListResult listFiles(@QueryParam("filePath") String filePath,
                                    @QueryParam("credId") String cloudCredId) {
        try {
            if (StringUtils.isBlank(filePath)) {
                throw BeaconWebException.newAPIException("FS Path can't be empty");
            }
            if (StringUtils.isBlank(cloudCredId)) {
                LOG.info("List FS path {} details on cluster {}", filePath, ClusterHelper.getLocalCluster().getName());
                return listFiles(ClusterHelper.getLocalCluster(), filePath);
            } else {
                LOG.info("List Cloud path {} details on cred id {}", filePath, cloudCredId);
                return listCloudFiles(filePath, cloudCredId);
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
        if (conf.get(BeaconConstants.NN_PRINCIPAL) != null) {
            conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
                    conf.get(BeaconConstants.NN_PRINCIPAL));
        }
        try {
            //Get groups for the API user using namenode's getGroupsForUser RPC
            String userName = RequestContext.get().getUser();
            GetUserMappingsProtocol proxy = NameNodeProxies.createProxy(conf, FileSystem.getDefaultUri(conf),
                    GetUserMappingsProtocol.class).getProxy();
            String[] userGroups = proxy.getGroupsForUser(userName);
            LOG.debug("Groups for user {}: {}", userName, StringUtils.join(userGroups, ", "));

            //Get dfs.permissions.superusergroup from hadoop conf
            String superUserGroups = conf.getTrimmed("dfs.permissions.superusergroup");
            LOG.debug("Super user groups for hdfs(dfs.permissions.superusergroup): {}", superUserGroups);
            List<String> superUserGroupsList = Arrays.asList(superUserGroups.split("\\s*,\\s*"));

            //Intersection of user groups and super user groups implies that the API user is HDFS superuser
            Collection intersection = CollectionUtils.intersection(Arrays.asList(userGroups), superUserGroupsList);
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
                throw BeaconWebException.newAPIException("Query param [filterBy] cannot be null or empty");
            }
            return getPolicyLogsInternal(filters, startStr, endStr, frequency, numLogs);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    private FileListResult listFiles(Cluster cluster, String path) throws BeaconException {
        try {
            return datasetListing.listFiles(cluster, path);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private FileListResult listCloudFiles(String path, String cloudCredId) throws BeaconException {
        path = ReplicationPolicyBuilder.appendCloudSchema(path, cloudCredId, SchemeType.HCFS_NAME);
        BeaconCloudCred cloudCred = new BeaconCloudCred(cloudCredDao.getCloudCred(cloudCredId));
        Configuration configuration = cloudCred.getHadoopConf();
        merge(configuration, cloudCred.getBucketEndpointConf(path));
        return datasetListing.listCloudFiles(cloudCred.getProvider(), configuration, path);
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
