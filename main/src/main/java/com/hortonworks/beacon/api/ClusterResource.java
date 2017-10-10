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
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.ClusterProperties;
import com.hortonworks.beacon.entity.ClusterValidator;
import com.hortonworks.beacon.entity.EntityValidatorFactory;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.ClusterPersistenceHelper;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreService;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.EntityManager;
import java.util.List;
import java.util.Properties;

/**
 * Cluster update request processing.
 */
class ClusterResource {

    private static final BeaconLog LOG = BeaconLog.getLog(ClusterResource.class);

    ClusterResource() {
    }

    void update(String name, PropertiesIgnoreCase properties) {
        LOG.debug("Cluster update processing started.");
        EntityManager entityManager = null;
        try {
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
            BeaconStoreService storeService = Services.get().getService(BeaconStoreService.SERVICE_NAME);
            entityManager = storeService.getEntityManager();
            entityManager.getTransaction().begin();
            ClusterPersistenceHelper.persistUpdatedCluster(updatedCluster, updatedProps, newProps, entityManager);
            entityManager.getTransaction().commit();
            LOG.debug("Cluster update processing completed.");
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e);
        } finally {
            if (entityManager != null && entityManager.getTransaction().isActive()) {
                entityManager.getTransaction().rollback();
            }
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
                throw new ValidationException(MessageCode.MAIN_000168.name(), prop);
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
}
