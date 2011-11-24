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

import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.EntityValidator;
import com.hortonworks.beacon.entity.EntityValidatorFactory;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.CloudCredDao;
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
    protected static CloudCredDao cloudCredDao = new CloudCredDao();

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

    protected void checkActivePolicies(String localClusterName, String remoteClusterName) throws ValidationException {
        boolean exists = policyDao.activePairedClusterPolicies(localClusterName,
                remoteClusterName);
        if (exists) {
            throw new ValidationException("Active policies are present. Operation can not be performed.");
        }
    }

    protected void checkActivePolicies(String cloudCred) throws BeaconException {
        boolean exists = policyDao.activePairedCloudPolicies(cloudCred);
        if (exists) {
            throw new ValidationException("Active cloud policies are present. Operation can not be performed.");
        }
    }
}
