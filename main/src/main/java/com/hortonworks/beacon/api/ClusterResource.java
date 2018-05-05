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
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.ClusterProperties;
import com.hortonworks.beacon.entity.ClusterValidator;
import com.hortonworks.beacon.entity.EntityValidatorFactory;
import com.hortonworks.beacon.entity.exceptions.EntityAlreadyExistsException;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.events.BeaconEvents;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.service.PluginManagerService;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.util.ClusterStatus;
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
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

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
        try {
            requestProperties.load(request.getInputStream());
            String localStr = requestProperties.getPropertyIgnoreCase(Cluster.ClusterFields.LOCAL.getName());
            APIResult result = submitCluster(ClusterBuilder.buildCluster(requestProperties, clusterName));
            if (APIResult.Status.SUCCEEDED == result.getStatus()
                    && Services.get().isRegistered(PluginManagerService.class.getName())
                    && Boolean.parseBoolean(localStr)) {
                // Register all the plugins
                Services.get().getService(PluginManagerService.class).registerPlugins();
            }
            return result;
        } catch (EntityAlreadyExistsException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.CONFLICT);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @PUT
    @Path("{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult update(@PathParam("cluster-name") String clusterName, @Context HttpServletRequest request) {
        try {
            PropertiesIgnoreCase properties = new PropertiesIgnoreCase();
            properties.load(request.getInputStream());
            update(clusterName, properties);
            return new APIResult(APIResult.Status.SUCCEEDED, "Cluster [{}] update request succeeded.", clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public ClusterList list(@DefaultValue("") @QueryParam("fields") String fields,
                            @DefaultValue("name") @QueryParam("orderBy") String orderBy,
                            @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                            @DefaultValue("0") @QueryParam("offset") Integer offset,
                            @QueryParam("numResults") Integer resultsPerPage) {
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
        offset = checkAndSetOffset(offset);
        return getClusterList(fields, orderBy, sortOrder, offset, resultsPerPage);
    }

    @GET
    @Path("status/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public StatusResult status(@PathParam("cluster-name") String clusterName) {
        try {
            return getClusterStatus(clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("getEntity/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public String definition(@PathParam("cluster-name") String clusterName) {
        return getClusterDefinition(clusterName);
    }

    @DELETE
    @Path("delete/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult delete(@PathParam("cluster-name") String clusterName) {
        try {
            return deleteCluster(clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("pair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult pair(@QueryParam("remoteClusterName") String remoteClusterName,
                          @DefaultValue("false") @QueryParam("isInternalPairing") boolean isInternalPairing) {
        if (StringUtils.isBlank(remoteClusterName)) {
            throw BeaconWebException.newAPIException("Query params remoteClusterName cannot be null or empty");
        }

        try {
            return pairClusters(remoteClusterName, isInternalPairing);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("unpair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult unPair(@QueryParam("remoteClusterName") String remoteClusterName,
                            @DefaultValue("false") @QueryParam("isInternalUnpairing")
                                            boolean isInternalUnpairing) {
        if (StringUtils.isBlank(remoteClusterName)) {
            throw BeaconWebException.newAPIException("Query params remoteClusterName cannot be null or empty");
        }

        try {
            return unpairClusters(remoteClusterName, isInternalUnpairing);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    private synchronized APIResult submitCluster(Cluster cluster) throws BeaconException {
        try {
            RequestContext.get().startTransaction();
            validate(cluster);
            ValidationUtil.validateEncryptionAlgorithmType(cluster);
            clusterDao.submitCluster(cluster);
            BeaconEvents.createEvents(Events.SUBMITTED, EventEntityType.CLUSTER, cluster);
            RequestContext.get().commitTransaction();
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful {}: {}", cluster.getEntityType(),
                cluster.getName());
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private ClusterList getClusterList(String fieldStr, String orderBy, String sortOrder, Integer offset,
                                       Integer resultsPerPage) {
        try {
            return clusterDao.getFilteredClusters(fieldStr, orderBy, sortOrder, offset, resultsPerPage);
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e);
        }
    }

    private StatusResult getClusterStatus(String clusterName) {
        return new StatusResult(clusterName, Entity.EntityStatus.SUBMITTED.name());
    }

    private String getClusterDefinition(String entityName) {
        try {
            Entity entity = clusterDao.getActiveCluster(entityName);
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
            Cluster cluster = clusterDao.getActiveCluster(clusterName);
            if (StringUtils.isNotBlank(cluster.getPeers())) {
                throw new ValidationException("Can't delete cluster {} as its paired with {}", clusterName,
                        cluster.getPeers());
            }
            clusterDao.deleteCluster(cluster);
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

    private APIResult pairClusters(String remoteClusterName, boolean isInternalPairing) throws BeaconException {
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
            remoteCluster = clusterDao.getActiveCluster(remoteClusterName);
            if (remoteCluster == null) {
                throw BeaconWebException.newAPIException(Response.Status.NOT_FOUND,
                        "For pairing both local {} and remote cluster {} should be submitted.",
                        localClusterName, remoteClusterName);
            }

            // Check if cluster are already paired.
            boolean areClustersPaired = ClusterHelper.areClustersPaired(localCluster, remoteClusterName);
            ValidationUtil.validateClusterPairing(localCluster, remoteCluster);
            if (!areClustersPaired) {
                clusterDao.pairCluster(localCluster, remoteCluster);
            }
            if (!isInternalPairing) {
                BeaconWebClient remoteClient = new BeaconWebClient(remoteCluster.getBeaconEndpoint(),
                        remoteCluster.getKnoxGatewayURL());
                pairClustersInRemote(remoteClient, remoteClusterName, localClusterName);
            }
            BeaconEvents.createEvents(Events.PAIRED, EventEntityType.CLUSTER,
                getClusterWithPeerInfo(localCluster, remoteClusterName));
            RequestContext.get().commitTransaction();
            return new APIResult(APIResult.Status.SUCCEEDED, "Clusters successfully paired");
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException | BeaconException e) {
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
            remoteCluster = clusterDao.getActiveCluster(remoteClusterName);
            localClusterName = localCluster.getName();
            areClustersPaired = ClusterHelper.areClustersPaired(localCluster, remoteClusterName);
            if (areClustersPaired) {
                // Check active policies between the paired clusters.
                checkActivePolicies(localClusterName, remoteClusterName);
                // Update local cluster with paired information so that it gets pushed to remote
                clusterDao.unpairPairedCluster(localCluster, remoteCluster);
            }
            if (!isInternalUnpairing) {
                BeaconWebClient remoteClient = new BeaconWebClient(remoteCluster.getBeaconEndpoint(),
                        remoteCluster.getKnoxGatewayURL());
                unpairClustersInRemote(remoteClient, remoteClusterName, localClusterName);
            }
            BeaconEvents.createEvents(Events.UNPAIRED, EventEntityType.CLUSTER,
                getClusterWithPeerInfo(localCluster, remoteClusterName));
            RequestContext.get().commitTransaction();
            return new APIResult(APIResult.Status.SUCCEEDED, "Clusters successfully unpaired");
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
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
            Cluster modifiedExistingCluster = existingCluster;
            addNewPropsAndUpdateOlderPropsValue(modifiedExistingCluster, updatedCluster, newProps, updatedProps);

            // Update the pairing status as required
            validatePairingAndUpdateStatus(modifiedExistingCluster);

            // persist cluster update information
            clusterDao.persistUpdatedCluster(updatedCluster, updatedProps, newProps);
            RequestContext.get().commitTransaction();
            LOG.debug("Cluster update processing completed.");
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private void addNewPropsAndUpdateOlderPropsValue(Cluster modifiedExistingCluster, Cluster updatedCluster,
                                                     PropertiesIgnoreCase newProps, PropertiesIgnoreCase updatedProps) {
        Properties customProps = modifiedExistingCluster.getCustomProperties();
        customProps.putAll(newProps);
        customProps.putAll(updatedProps);
        if (StringUtils.isNotBlank(updatedCluster.getAtlasEndpoint())) {
            modifiedExistingCluster.setAtlasEndpoint(updatedCluster.getAtlasEndpoint());
        }
        if (StringUtils.isNotBlank(updatedCluster.getBeaconEndpoint())) {
            modifiedExistingCluster.setBeaconEndpoint(updatedCluster.getBeaconEndpoint());
        }
        if (StringUtils.isNotBlank(updatedCluster.getFsEndpoint())) {
            modifiedExistingCluster.setFsEndpoint(updatedCluster.getFsEndpoint());
        }
        if (StringUtils.isNotBlank(updatedCluster.getHsEndpoint())) {
            modifiedExistingCluster.setHsEndpoint(updatedCluster.getHsEndpoint());
        }
        if (StringUtils.isNotBlank(updatedCluster.getRangerEndpoint())) {
            modifiedExistingCluster.setRangerEndpoint(updatedCluster.getRangerEndpoint());
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
        ValidationUtil.validateEncryptionAlgorithmType(updatedCluster);
        LOG.debug("Validation completed updated cluster.");
    }

    private void validatePairingAndUpdateStatus(Cluster modifiedExistingCluster) throws BeaconException {
        String peersStr = modifiedExistingCluster.getPeers();
        if (StringUtils.isBlank(peersStr)) {
            LOG.info("No peer for cluster [{}] found, skipping the pairing status validation",
                    modifiedExistingCluster.getName());
            return;
        }
        String[] peers = modifiedExistingCluster.getPeers().split(BeaconConstants.COMMA_SEPARATOR);
        Set<String> toBeSuspendedPeers = new HashSet<String>();
        Set<String> toBePairedBackPeers = new HashSet<String>();
        for (String peer: peers) {
            Cluster remoteCluster = null;
            try {
                remoteCluster = ClusterHelper.getActiveCluster(peer);
                ValidationUtil.validateClusterPairing(modifiedExistingCluster, remoteCluster);
                toBePairedBackPeers.add(remoteCluster.getName());
            } catch (ValidationException e){
                LOG.error("Validation for existing pairing for remote cluster{} failed, will suspend the pairing "
                        + "status", peer, e);
                toBeSuspendedPeers.add(peer);
            } catch (BeaconException e) {
                LOG.warn("Exception while Validating for existing pairing for remote cluster{}", peer, e);
                throw e;
            }
        }
        if (!toBeSuspendedPeers.isEmpty()) {
            clusterDao.movePairStatusForClusters(modifiedExistingCluster, toBeSuspendedPeers, ClusterStatus.PAIRED,
                    ClusterStatus.SUSPENDED);
        }
        if (!toBePairedBackPeers.isEmpty()) {
            clusterDao.movePairStatusForClusters(modifiedExistingCluster, toBePairedBackPeers, ClusterStatus.SUSPENDED,
                    ClusterStatus.PAIRED);
        }
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
            throw BeaconWebException.newAPIException(Response.Status.fromStatusCode(e.getStatus()), e,
                    "Remote cluster {} returned error: ", remoteClusterName);
        } catch (Exception e) {
            LOG.error("Exception while pairing local cluster to remote");
            throw e;
        }
    }

    // TODO : In future when house keeping async is added ignore any errors as this will be retried async
    private void unpairClustersInRemote(BeaconWebClient remoteClient, String remoteClusterName,
                                        String localClusterName) {
        try {
            remoteClient.unpairClusters(localClusterName, true);
        } catch (BeaconClientException e) {
            throw BeaconWebException.newAPIException(Response.Status.fromStatusCode(e.getStatus()), e,
                    "Remote cluster {} returned error: ", remoteClusterName);
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
