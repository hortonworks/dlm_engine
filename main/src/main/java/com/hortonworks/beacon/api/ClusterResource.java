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

import com.codahale.metrics.annotation.Timed;
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
import com.hortonworks.beacon.entity.BeaconCluster;
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
import com.hortonworks.beacon.store.bean.ClusterPairBean;
import com.hortonworks.beacon.util.ClusterStatus;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Beacon cluster resource management operations as REST API. Root resource (exposed at "myresource" path).
 */
@Path("/api/beacon/cluster")
public class ClusterResource extends AbstractResourceManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterResource.class);

    /**
     * Submit a new cluster to beacon.
     * @param clusterName
     * @param requestProperties
     * @return
     */
    @POST
    @Path("submit/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Timed(absolute = true, name="api.beacon.cluster.submit")
    public APIResult submit(@PathParam("cluster-name") String clusterName, PropertiesIgnoreCase requestProperties) {
        try {
            APIResult result = submitCluster(ClusterBuilder.buildCluster(requestProperties, clusterName));
            return result;
        } catch (EntityAlreadyExistsException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.CONFLICT);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Update an existing cluster.
     * @param clusterName
     * @param requestProperties
     * @return
     */
    @PUT
    @Path("{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Timed(absolute = true, name="api.beacon.cluster.update")
    public APIResult update(@PathParam("cluster-name") String clusterName, PropertiesIgnoreCase requestProperties) {
        try {
            updateInternal(clusterName, requestProperties);
            return new APIResult(APIResult.Status.SUCCEEDED, "Cluster [{}] update request succeeded.", clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * List all clusters.
     * @param fields see {@link ClusterList.ClusterFieldList}.
     * @param orderBy
     * @param sortOrder
     * @param offset
     * @param resultsPerPage
     * @return
     */
    @GET
    @Path("list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Timed(absolute = true, name="api.beacon.cluster.list")
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

    /**
     * Get the status of the cluster.
     * @param clusterName
     * @return
     */
    @GET
    @Path("status/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Timed(absolute = true, name="api.beacon.cluster.status")
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
    @Timed(absolute = true, name="api.beacon.cluster.definition")
    public Cluster definition(@PathParam("cluster-name") String clusterName) {
        return getClusterDefinition(clusterName);
    }

    @DELETE
    @Path("delete/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Timed(absolute = true, name="api.beacon.cluster.delete")
    public APIResult delete(@PathParam("cluster-name") String clusterName) {
        try {
            return deleteCluster(clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Pair the local cluster with other remote cluster.
     * @param remoteClusterName name of remote cluster.
     * @param isInternalPairing True means sync call from the remote beacon. False means
     *                          call is coming from DLM App.
     * @return
     */
    @POST
    @Path("pair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Timed(absolute = true, name="api.beacon.cluster.pair")
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

    /**
     * Unpair the local cluster from the remote cluster.
     * @param remoteClusterName name of remote cluster.
     * @param isInternalUnpairing True means sync call from the remote beacon. False means
     *      *                     call is coming from DLM App.
     * @return
     */
    @POST
    @Path("unpair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Timed(absolute = true, name="api.beacon.cluster.unpair")
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
            // BUG-108819: Beacon doesn't validate the remote cluster anymore
            if (cluster.isLocal()) {
                validate(cluster);
            } else {
                ClusterValidator validator = new ClusterValidator();
                validator.validateClusterExists(cluster.getName());
            }
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

    private Cluster getClusterDefinition(String entityName) {
        try {
            return clusterDao.getActiveCluster(entityName);
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
            if (cluster.getPeers().size() > 0) {
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
                        new BeaconCluster(remoteCluster).getKnoxGatewayURL());
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
                        new BeaconCluster(remoteCluster).getKnoxGatewayURL());
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

    private void updateInternal(String name, PropertiesIgnoreCase properties) throws BeaconException {
        LOG.debug("Cluster update processing started.");
        try {
            RequestContext.get().startTransaction();
            // Prepare Cluster objects for existing and updated request.
            Cluster existingCluster = ClusterHelper.getActiveCluster(name);
            Cluster updatedCluster = ClusterBuilder.buildCluster(properties);
            updatedCluster.setName(existingCluster.getName());
            updatedCluster.setVersion(existingCluster.getVersion());
            updatedCluster.setLocal(existingCluster.isLocal());
            updatedCluster.setPeers(existingCluster.getPeers());
            updatedCluster.setPeersInfo(existingCluster.getPeersInfo());

            // Validation of the update request for local cluster
            if (updatedCluster.isLocal()) {
                validateUpdate(existingCluster, updatedCluster, properties);
            }

            // Prepare for cluster update into store
            PropertiesIgnoreCase updatedProps = new PropertiesIgnoreCase();
            PropertiesIgnoreCase newProps = new PropertiesIgnoreCase();
            PropertiesIgnoreCase deletedProps = new PropertiesIgnoreCase();
            findUpdatedAndNewCustomProps(updatedCluster, existingCluster, updatedProps, newProps, deletedProps);
            addNewPropsAndUpdateOlderPropsValue(updatedCluster, newProps, updatedProps);

            // Update the pairing status as required
            validatePairingAndUpdateStatus(updatedCluster);

            // persist cluster update information
            clusterDao.persistUpdatedCluster(updatedCluster, updatedProps, newProps, deletedProps);
            RequestContext.get().commitTransaction();
            LOG.debug("Cluster update processing completed.");
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private void addNewPropsAndUpdateOlderPropsValue(Cluster updatedCluster,
                                                     PropertiesIgnoreCase newProps, PropertiesIgnoreCase updatedProps) {
        Properties customProps = updatedCluster.getCustomProperties();
        customProps.putAll(newProps);
        customProps.putAll(updatedProps);
    }

    private void findUpdatedAndNewCustomProps(Cluster updatedCluster, Cluster existingCluster,
                                              PropertiesIgnoreCase updatedProps, PropertiesIgnoreCase newProps,
                                              PropertiesIgnoreCase deletedProps) {
        Properties existingClusterCustomProps = existingCluster.getCustomProperties();
        Properties updatedClusterCustomProps = updatedCluster.getCustomProperties();
        for (String property : existingClusterCustomProps.stringPropertyNames()) {
            if (updatedClusterCustomProps.containsKey(property)){
                updatedProps.setProperty(property, updatedClusterCustomProps.getProperty(property));
                updatedClusterCustomProps.remove(property);
            } else {
                deletedProps.setProperty(property, existingClusterCustomProps.getProperty(property));
            }
        }
        for (String property : updatedClusterCustomProps.stringPropertyNames()) {
            newProps.setProperty(property, updatedClusterCustomProps.getProperty(property));
        }
    }

    private void validateUpdate(Cluster existingCluster, Cluster updatedCluster, PropertiesIgnoreCase properties)
            throws BeaconException {
        LOG.debug("Validation begin updated cluster.");
        validateExclusionProp(existingCluster, properties);
        ClusterValidator clusterValidator = new ClusterValidator();
        clusterValidator.validateClusterInfo(updatedCluster, true);
        validateEndPoints(updatedCluster);
        ValidationUtil.validateEncryptionAlgorithmType(updatedCluster);
        LOG.debug("Validation completed updated cluster.");
    }

    private void validatePairingAndUpdateStatus(Cluster updatedCluster) throws BeaconException {
        List<String> peers = updatedCluster.getPeers();
        if (peers.isEmpty()) {
            LOG.info("No peer for cluster [{}] found, skipping the pairing status validation",
                    updatedCluster.getName());
            return;
        }
        List<ClusterPairBean> clusterPairBeans = clusterDao.getPairedCluster(updatedCluster);
        for (String peer: peers) {
            Cluster pairedCluster = null;
            ClusterPairBean clusterPairBean = null;
            try {
                clusterPairBean = getClusterPairBean(clusterPairBeans, peer);
                pairedCluster = ClusterHelper.getActiveCluster(peer);
                if (updatedCluster.isLocal()) {
                    ValidationUtil.validateClusterPairing(updatedCluster, pairedCluster);
                } else {
                    ValidationUtil.validateClusterPairing(pairedCluster, updatedCluster);
                }
                if (!ClusterStatus.PAIRED.name().equals(clusterPairBean.getStatus())) {
                    clusterDao.updatePairStatus(clusterPairBean, ClusterStatus.PAIRED);
                    BeaconEvents.createEvents(Events.PAIRED, EventEntityType.CLUSTER, updatedCluster);
                }
            } catch (ValidationException e){
                LOG.error("Validation for existing pairing for remote cluster{} failed, will suspend the pairing "
                        + "status", peer, e);
                clusterDao.updatePairStatus(clusterPairBean, ClusterStatus.SUSPENDED, e.getMessage());
                String message = "Pairing between cluster " + updatedCluster.getName() + " and cluster " + peer
                        + " is suspended due to " + e.getMessage();
                BeaconEvents.createEvents(Events.SUSPENDED, message, EventEntityType.CLUSTER, updatedCluster);
            } catch (BeaconException e) {
                LOG.warn("Exception while Validating for existing pairing for remote cluster{}", peer, e);
                throw e;
            }
        }
    }

    private ClusterPairBean getClusterPairBean(List<ClusterPairBean> clusterPairBeans, String peer)
            throws BeaconException {
        for(ClusterPairBean clusterPairBean: clusterPairBeans) {
            if (clusterPairBean.getClusterName().equalsIgnoreCase(peer)
                    || clusterPairBean.getPairedClusterName().equalsIgnoreCase(peer)) {
                return clusterPairBean;
            }
        }
        throw new BeaconException("Unable to find Cluster pair info for peer:" + peer);
    }

    void validateExclusionProp(Cluster existingCluster, PropertiesIgnoreCase properties) throws ValidationException {
        List<String> exclusionProps = ClusterProperties.updateExclusionProps();
        for (String prop : exclusionProps) {
            if (properties.getPropertyIgnoreCase(prop) != null) {
                throw new ValidationException("Property [{}] is not allowed to be updated.", prop);
            }
        }
        if (properties.containsKey(ClusterProperties.LOCAL.getName())) {
            boolean isLocal = Boolean.valueOf(properties.getPropertyIgnoreCase(ClusterProperties.LOCAL.getName()));
            if (isLocal != existingCluster.isLocal()) {
                throw new ValidationException("Property [{}] is not allowed to be updated.",
                        ClusterProperties.LOCAL.getName());
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
            LOG.error("Error in pairing", e);
            Response.Status status = Response.Status.fromStatusCode(e.getStatus());
            if (status == null) {
                status = Response.Status.INTERNAL_SERVER_ERROR;
            }
            throw BeaconWebException.newAPIException(status, e, "Remote cluster {} returned error: ",
                    remoteClusterName);
        } catch (Exception e) {
            LOG.error("Exception while unpairing local cluster to remote");
            throw e;
        }
    }

    private Cluster getClusterWithPeerInfo(Cluster localCluster, String remoteClusterName) {
        List<String> oldPeers = localCluster.getPeers();
        oldPeers.add(remoteClusterName);
        localCluster.setPeers(oldPeers);
        return localCluster;
    }
}
