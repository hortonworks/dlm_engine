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

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.api.result.DBListResult;
import com.hortonworks.beacon.api.result.FileListResult;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.rb.MessageCode;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Beacon resource management operations as REST API. Root resource (exposed at "myresource" path).
 */
@Path("/api/beacon")
public class BeaconResource extends AbstractResourceManager {

    private static final BeaconLog LOG = BeaconLog.getLog(BeaconResource.class);

    @GET
    @Path("file/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public FileListResult listFiles(@QueryParam("path") String path, @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        List<String> keys = Collections.singletonList("path");
        List<String> values = Collections.singletonList(path);
        LOG.info(MessageCode.MAIN_000167.name(), super.concatKeyValue(keys, values));
        try {
            if (StringUtils.isBlank(path)) {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000159.name());
            }
            LOG.info(MessageCode.MAIN_000161.name(), path, ClusterHelper.getLocalCluster().getName());
            return listFiles(ClusterHelper.getLocalCluster(), path);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("hive/listDBs")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public DBListResult listHiveDBs(@Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        try {
            LOG.info(MessageCode.MAIN_000162.name(), ClusterHelper.getLocalCluster().getName());
            return listHiveDBs(ClusterHelper.getLocalCluster());
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("hive/listTables")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public DBListResult listHiveTables(@QueryParam("db") String dbName, @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        List<String> keys = Collections.singletonList("database");
        List<String> values = Collections.singletonList(dbName);
        LOG.info(MessageCode.MAIN_000167.name(), super.concatKeyValue(keys, values));
        try {
            if (StringUtils.isBlank(dbName)) {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000160.name());
            }

            LOG.info(MessageCode.MAIN_000162.name(), ClusterHelper.getLocalCluster().getName());
            return listHiveTables(ClusterHelper.getLocalCluster(), dbName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
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
                                            @DefaultValue("false") @QueryParam("archived") String archived,
                                            @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        List<String> keys = Arrays.asList("filterBy", "orderBy", "sortBy", "offset", "numResults", "archived");
        List<String> values = Arrays.asList(filters, orderBy, sortBy, offset.toString(),
                resultsPerPage.toString(), archived);
        LOG.info(MessageCode.MAIN_000167.name(), super.concatKeyValue(keys, values));
        try {
            boolean isArchived = Boolean.parseBoolean(archived);
            return listInstance(filters, orderBy, sortBy, offset, resultsPerPage, isArchived);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }


    private FileListResult listFiles(Cluster cluster, String path) throws BeaconException {
        try {
            return DataListHelper.listFiles(cluster, path);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private DBListResult listHiveDBs(Cluster cluster) throws BeaconException {
        try {
            return DataListHelper.listHiveDBDetails(cluster, " ");
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private DBListResult listHiveTables(Cluster cluster, String dbName) throws BeaconException {
        try {
            return DataListHelper.listHiveDBDetails(cluster, dbName);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }
}
