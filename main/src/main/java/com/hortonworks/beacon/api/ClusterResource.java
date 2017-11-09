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
import com.hortonworks.beacon.api.util.ValidationUtil;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.BeaconWebClient;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.StatusResult;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.ClusterProperties;
import com.hortonworks.beacon.entity.ClusterValidator;
import com.hortonworks.beacon.entity.EntityValidatorFactory;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.ClusterPersistenceHelper;
import com.hortonworks.beacon.events.BeaconEvents;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.plugin.service.PluginManagerService;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Beacon cluster resource management operations as REST API. Root resource (exposed at "myresource" path).
 */
@Path("/api/beacon/cluster")
public class ClusterResource extends AbstractResourceManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterResource.class);

    @POST
    @Path("submit/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submit(@PathParam("cluster-name") String clusterName, @Context HttpServletRequest request) {
        PropertiesIgnoreCase requestProperties = new PropertiesIgnoreCase();
        BeaconLogUtils.createPrefix((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                clusterName);
        try {
            requestProperties.load(request.getInputStream());
            LOG.info("Request Parameters: {}", requestProperties);
            String localStr = requestProperties.getPropertyIgnoreCase(Cluster.ClusterFields.LOCAL.getName());
            APIResult result = submitCluster(ClusterBuilder.buildCluster(requestProperties, clusterName));
            if (APIResult.Status.SUCCEEDED == result.getStatus()
                    && Services.get().isRegistered(PluginManagerService.SERVICE_NAME)
                    && Boolean.parseBoolean(localStr)) {
                // Register all the plugins
                ((PluginManagerService) Services.get()
                        .getService(PluginManagerService.SERVICE_NAME)).registerPlugins();
            }
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        } finally{
            BeaconLogUtils.deletePrefix();
        }
    }

    @PUT
    @Path("{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult update(@PathParam("cluster-name") String clusterName, @Context HttpServletRequest request) {
        BeaconLogUtils.createPrefix((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                clusterName);
        try {
            PropertiesIgnoreCase properties = new PropertiesIgnoreCase();
            properties.load(request.getInputStream());
            LOG.info("Request Parameters: {}", properties);
            update(clusterName, properties);
            return new APIResult(APIResult.Status.SUCCEEDED, "Cluster [{}] update request succeeded.", clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        } finally{
            BeaconLogUtils.deletePrefix();
        }
    }

    @GET
    @Path("list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public ClusterList list(@DefaultValue("") @QueryParam("fields") String fields,
                            @DefaultValue("name") @QueryParam("orderBy") String orderBy,
                            @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                            @DefaultValue("0") @QueryParam("offset") Integer offset,
                            @QueryParam("numResults") Integer resultsPerPage,
                            @Context HttpServletRequest request) {
        BeaconLogUtils.createPrefix((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        List<String> keys = Arrays.asList("fields", "orderBy", "sortOrder", "offset", "resultsPerPage");
        List<String> values = Arrays.asList(fields, orderBy, sortOrder,
                offset.toString(), resultsPerPage.toString());
        LOG.info("Request Parameters: {}", concatKeyValue(keys, values));
        resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
        offset = checkAndSetOffset(offset);
        BeaconLogUtils.deletePrefix();
        return getClusterList(fields, orderBy, sortOrder, offset, resultsPerPage);
    }

    @GET
    @Path("status/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public StatusResult status(@PathParam("cluster-name") String clusterName,
                               @Context HttpServletRequest request) {
        BeaconLogUtils.createPrefix((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        List<String> keys = Collections.singletonList("clusterName");
        List<String> values = Collections.singletonList(clusterName);
        LOG.info("Request Parameters: {}", concatKeyValue(keys, values));
        try {
            return getClusterStatus(clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        } finally{
            BeaconLogUtils.deletePrefix();
        }
    }

    @GET
    @Path("getEntity/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public String definition(@PathParam("cluster-name") String clusterName, @Context HttpServletRequest request) {
        BeaconLogUtils.createPrefix((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        List<String> keys = Collections.singletonList("clusterName");
        List<String> values = Collections.singletonList(clusterName);
        LOG.info("Request Parameters: {}", concatKeyValue(keys, values));
        BeaconLogUtils.deletePrefix();
        return getClusterDefinition(clusterName);
    }

    @DELETE
    @Path("delete/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult delete(@PathParam("cluster-name") String clusterName,
                            @Context HttpServletRequest request) {
        BeaconLogUtils.createPrefix((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        List<String> keys = Collections.singletonList("clusterName");
        List<String> values = Collections.singletonList(clusterName);
        LOG.info("Request Parameters: {}", concatKeyValue(keys, values));
        try {
            return deleteCluster(clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        } finally{
            BeaconLogUtils.deletePrefix();
        }
    }

    @POST
    @Path("pair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult pair(@QueryParam("remoteClusterName") String remoteClusterName,
                          @DefaultValue("false") @QueryParam("isInternalPairing") boolean isInternalPairing,
                          @Context HttpServletRequest request) {
        BeaconLogUtils.createPrefix((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        List<String> keys = Collections.singletonList("remoteClusterName");
        List<String> values = Collections.singletonList(remoteClusterName);
        LOG.info("Request Parameters: {}", concatKeyValue(keys, values));
        if (StringUtils.isBlank(remoteClusterName)) {
            BeaconLogUtils.deletePrefix();
            throw BeaconWebException.newAPIException("Query params remoteClusterName cannot be null or empty");
        }

        try {
            return pairClusters(remoteClusterName, isInternalPairing);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        } finally{
            BeaconLogUtils.deletePrefix();
        }
    }

    @POST
    @Path("unpair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult unPair(@QueryParam("remoteClusterName") String remoteClusterName,
                            @DefaultValue("false") @QueryParam("isInternalUnpairing")
                                            boolean isInternalUnpairing, @Context HttpServletRequest request) {
        BeaconLogUtils.createPrefix((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        List<String> keys = Collections.singletonList("remoteClusterName");
        List<String> values = Collections.singletonList(remoteClusterName);
        LOG.info("Request Parameters: {}", concatKeyValue(keys, values));
        if (StringUtils.isBlank(remoteClusterName)) {
            BeaconLogUtils.deletePrefix();
            throw BeaconWebException.newAPIException("Query params remoteClusterName cannot be null or empty");
        }

        try {
            return unpairClusters(remoteClusterName, isInternalUnpairing);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        } finally{
            BeaconLogUtils.deletePrefix();
        }
    }

    private synchronized APIResult submitCluster(Cluster cluster) {
        try {
            RequestContext.get().startTransaction();
            validate(cluster);
            ClusterPersistenceHelper.submitCluster(cluster);
            BeaconEvents.createEvents(Events.SUBMITTED, EventEntityType.CLUSTER, cluster);
            RequestContext.get().commitTransaction();
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful {}: {}", cluster.getEntityType(),
                cluster.getName());
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private ClusterList getClusterList(String fieldStr, String orderBy, String sortOrder, Integer offset,
                                       Integer resultsPerPage) {
        try {
            return ClusterPersistenceHelper.getFilteredClusters(fieldStr, orderBy, sortOrder, offset, resultsPerPage);
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e);
        }
    }

    private StatusResult getClusterStatus(String clusterName) {
        return new StatusResult(clusterName, Entity.EntityStatus.SUBMITTED.name());
    }

    private String getClusterDefinition(String entityName) {
        try {
            Entity entity = ClusterPersistenceHelper.getActiveCluster(entityName);
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(entity);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e);
        }
    }

    private APIResult deleteCluster(String clusterName) {
        try {
            RequestContext.get().startTransaction();
            Cluster cluster = ClusterPersistenceHelper.getActiveCluster(clusterName);
            ClusterPersistenceHelper.unpairAllPairedCluster(cluster);
            ClusterPersistenceHelper.deleteCluster(cluster);
            BeaconEvents.createEvents(Events.DELETED, EventEntityType.CLUSTER, cluster);
            RequestContext.get().commitTransaction();
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconException e) {
            throw BeaconWebException.newAPIException(e);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Cluster {} removed successfully.", clusterName);
    }

    private APIResult pairClusters(String remoteClusterName, boolean isInternalPairing) {
        Cluster localCluster;
        try {
            RequestContext.get().startTransaction();
            localCluster = ClusterHelper.getLocalCluster();
            // Compare local cluster name and remote cluster name.
            String localClusterName = localCluster.getName();
            if (localClusterName.equalsIgnoreCase(remoteClusterName)) {
                throw BeaconWebException.newAPIException(
                    "RemoteClusterName {} cannot be same as localClusterName {}. Cluster cannot be paired with itself",
                    remoteClusterName, localClusterName);
            }

            // Remote cluster should also be submitted (available) for paring.
            Cluster remoteCluster;
            remoteCluster = ClusterPersistenceHelper.getActiveCluster(remoteClusterName);
            if (remoteCluster == null) {
                throw BeaconWebException.newAPIException(
                    "For pairing both local {} and remote cluster {} should be submitted.", Response.Status.NOT_FOUND,
                    localClusterName, remoteClusterName);
            }

            // Check if cluster are already paired.
            boolean areClustersPaired = ClusterHelper.areClustersPaired(localCluster, remoteClusterName);
            ValidationUtil.validateClusterPairing(localCluster, remoteCluster);
            if (!areClustersPaired) {
                ClusterPersistenceHelper.pairCluster(localCluster, remoteCluster);
            }
            if (!isInternalPairing) {
                BeaconWebClient remoteClient = new BeaconWebClient(remoteCluster.getBeaconEndpoint());
                pairClustersInRemote(remoteClient, remoteClusterName, localClusterName);
            }
            BeaconEvents.createEvents(Events.PAIRED, EventEntityType.CLUSTER,
                getClusterWithPeerInfo(localCluster, remoteClusterName));
            RequestContext.get().commitTransaction();
            return new APIResult(APIResult.Status.SUCCEEDED, "Clusters successfully paired");
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private APIResult unpairClusters(String remoteClusterName, boolean isInternalUnpairing) {
        String localClusterName;
        Cluster localCluster;
        Cluster remoteCluster;
        boolean areClustersPaired;
        try {
            RequestContext.get().startTransaction();
            localCluster = ClusterHelper.getLocalCluster();
            remoteCluster = ClusterPersistenceHelper.getActiveCluster(remoteClusterName);
            localClusterName = localCluster.getName();
            areClustersPaired = ClusterHelper.areClustersPaired(localCluster, remoteClusterName);
            if (areClustersPaired) {
                // Check active policies between the paired clusters.
                checkActivePolicies(localClusterName, remoteClusterName);
                // Update local cluster with paired information so that it gets pushed to remote
                ClusterPersistenceHelper.unpairPairedCluster(localCluster, remoteCluster);
            }
            if (!isInternalUnpairing) {
                BeaconWebClient remoteClient = new BeaconWebClient(remoteCluster.getBeaconEndpoint());
                unpairClustersInRemote(remoteClient, remoteClusterName, localClusterName);
            }
            BeaconEvents.createEvents(Events.UNPAIRED, EventEntityType.CLUSTER,
                getClusterWithPeerInfo(localCluster, remoteClusterName));
            RequestContext.get().commitTransaction();
            return new APIResult(APIResult.Status.SUCCEEDED, "Clusters successfully unpaired");
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (RuntimeException | BeaconException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private void update(String name, PropertiesIgnoreCase properties) {
        LOG.debug("Cluster update processing started.");
        try {
            RequestContext.get().startTransaction();
            // Prepare Cluster objects for existing and updated request.
            Cluster existingCluster = ClusterHelper.getActiveCluster(name);
            Cluster updatedCluster = ClusterBuilder.buildCluster(properties);
            updatedCluster.setName(existingCluster.getName());
            updatedCluster.setVersion(existingCluster.getVersion());
            updatedCluster.setLocal(existingCluster.isLocal());

            // Validation of the update request
            validateUpdate(properties, updatedCluster);

            // Prepare for cluster update into store
            PropertiesIgnoreCase updatedProps = new PropertiesIgnoreCase();
            PropertiesIgnoreCase newProps = new PropertiesIgnoreCase();
            findUpdatedAndNewCustomProps(updatedCluster, existingCluster, updatedProps, newProps);

            // persist cluster update information
            ClusterPersistenceHelper.persistUpdatedCluster(updatedCluster, updatedProps, newProps);
            RequestContext.get().commitTransaction();
            LOG.debug("Cluster update processing completed.");
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private void findUpdatedAndNewCustomProps(Cluster updatedCluster, Cluster existingCluster,
                                              PropertiesIgnoreCase updatedProps, PropertiesIgnoreCase newProps) {
        Properties existingClusterCustomProps = existingCluster.getCustomProperties();
        Properties updatedClusterCustomProps = updatedCluster.getCustomProperties();
        for (String property : updatedClusterCustomProps.stringPropertyNames()) {
            if (existingClusterCustomProps.getProperty(property) != null) {
                updatedProps.setProperty(property, updatedClusterCustomProps.getProperty(property));
            } else {
                newProps.setProperty(property, updatedClusterCustomProps.getProperty(property));
            }
        }

        if (StringUtils.isNotBlank(updatedCluster.getTags())) {
            String existingClusterTags = existingCluster.getTags();
            String tags = StringUtils.isNotBlank(existingClusterTags)
                    ? existingClusterTags + BeaconConstants.COMMA_SEPARATOR + updatedCluster.getTags()
                    : updatedCluster.getTags();
            updatedCluster.setTags(tags);
        }
    }

    private void validateUpdate(PropertiesIgnoreCase properties, Cluster updatedCluster) throws BeaconException {
        LOG.debug("Validation begin updated cluster.");
        validateExclusionProp(properties);
        validateEndPoints(updatedCluster);
        LOG.debug("Validation completed updated cluster.");
    }

    void validateExclusionProp(PropertiesIgnoreCase properties) throws ValidationException {
        List<String> exclusionProps = ClusterProperties.updateExclusionProps();
        for (String prop : exclusionProps) {
            if (properties.getPropertyIgnoreCase(prop) != null) {
                throw new ValidationException("Property [{}] is not allowed to be updated.", prop);
            }
        }
    }

    private void validateEndPoints(Cluster cluster) throws BeaconException {
        ClusterValidator validator = (ClusterValidator) EntityValidatorFactory.getValidator(EntityType.CLUSTER);

        if (StringUtils.isNotBlank(cluster.getFsEndpoint())) {
            validator.validateFSInterface(cluster);
        }

        if (StringUtils.isNotBlank(cluster.getHsEndpoint())) {
            validator.validateHiveInterface(cluster);
        }
        // TODO : validation for Ranger and Atlas end points.
    }

    // TODO : In future when house keeping async is added ignore any errors as this will be retried async
    private void pairClustersInRemote(BeaconWebClient remoteClient, String remoteClusterName,
                                      String localClusterName) {
        try {
            remoteClient.pairClusters(localClusterName, true);
        } catch (BeaconClientException e) {
            throw BeaconWebException.newAPIException("Remote cluster {} returned error: ",
                Response.Status.fromStatusCode(e.getStatus()), e, remoteClusterName);
        } catch (Exception e) {
            LOG.error("Exception while pairing local cluster to remote");
            throw e;
        }
    }

    private void checkActivePolicies(String localClusterName, String remoteClusterName) {
        boolean exists = PersistenceHelper.activePairedClusterPolicies(localClusterName,
                remoteClusterName);
        if (exists) {
            throw BeaconWebException.newAPIException("Policies are present, unpair operation can not be done.");
        }
    }

    // TODO : In future when house keeping async is added ignore any errors as this will be retried async
    private void unpairClustersInRemote(BeaconWebClient remoteClient, String remoteClusterName,
                                        String localClusterName) {
        try {
            remoteClient.unpairClusters(localClusterName, true);
        } catch (BeaconClientException e) {
            throw BeaconWebException.newAPIException("Remote cluster {} returned error: ",
                Response.Status.fromStatusCode(e.getStatus()), e, remoteClusterName);
        } catch (Exception e) {
            LOG.error("Exception while unpairing local cluster to remote");
            throw e;
        }
    }

    private Cluster getClusterWithPeerInfo(Cluster localCluster, String remoteClusterName) {
        localCluster.setPeers(remoteClusterName);
        return localCluster;
    }
}
