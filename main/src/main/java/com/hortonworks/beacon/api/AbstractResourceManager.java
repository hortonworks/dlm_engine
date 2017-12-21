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

import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.EntityValidator;
import com.hortonworks.beacon.entity.EntityValidatorFactory;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.entity.util.PolicyDao;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.LogRetrieval;

/**
 * A base class for managing Beacon resource operations.
 */
abstract class AbstractResourceManager {
    protected BeaconConfig config = BeaconConfig.getInstance();
    protected PolicyDao policyDao = new PolicyDao();
    protected ClusterDao clusterDao = new ClusterDao();
    protected EventsDao eventsDao = new EventsDao();
    protected DatasetListing datasetListing = new DatasetListing();
    protected LogRetrieval logRetrieval = new LogRetrieval();

    PolicyInstanceList listInstance(String filters, String orderBy, String sortBy, Integer offset,
                                            Integer resultsPerPage, boolean isArchived) throws BeaconException {
        resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
        offset = checkAndSetOffset(offset);
        try {
            return policyDao.getFilteredJobInstance(filters, orderBy, sortBy,
                    offset, resultsPerPage, isArchived);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    Integer getDefaultResultsPerPage() {
        return config.getEngine().getResultsPerPage();
    }

    Integer getMaxInstanceCount() {
        return config.getEngine().getMaxInstanceCount();
    }

    Integer getMaxResultsPerPage() {
        return config.getEngine().getMaxResultsPerPage();
    }

    void validate(Entity entity) throws BeaconException {
        EntityValidator validator = EntityValidatorFactory.getValidator(entity.getEntityType());
        validator.validate(entity);
    }

    Integer checkAndSetOffset(Integer offset) {
        return (offset > 0) ? offset : 0;
    }
}
