/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.store.bean.ClusterBean;

import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Beacon store executor for Cluster listing.
 */
public class ClusterListExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterListExecutor.class);
    private static final String BASE_QUERY = "select OBJECT(b) from ClusterBean b where b.retirementTime IS NULL";
    private static final String COUNT_QUERY = "select COUNT(b.name) from ClusterBean b where b.retirementTime IS NULL";

    /**
     * Supported filterBy parameters for Cluster listing.
     */
    enum ClusterFilterBy {
        NAME("name"),
        DATACENTER("dataCenter");

        private String filterField;

        ClusterFilterBy(String filterField) {
            this.filterField = filterField;
        }

        private static String getFilterField(String name) {
            try {
                return valueOf(name.toUpperCase()).filterField;
            } catch (IllegalArgumentException e) {
                LOG.error(e.getMessage(), e);
                throw new IllegalArgumentException(
                        ResourceBundleService.getService().getString(MessageCode.PERS_000005.name(), name));
            }
        }
    }

    public List<ClusterBean> getFilterClusters(String orderBy, String sortOrder,
                                               Integer offset, Integer resultsPerPage) {
        Query query = createQuery(orderBy, sortOrder, offset, resultsPerPage, BASE_QUERY);
        List<ClusterBean> resultList = query.getResultList();
        return resultList;
    }

    public long getFilterClusterCount(Integer offset, Integer resultsPerPage) {
        Query query = createQuery(null, null, offset, resultsPerPage, COUNT_QUERY);
        return (long) query.getSingleResult();
    }

    private Query createQuery(String orderBy, String sortBy, Integer offset, Integer limitBy, String baseQuery) {
        StringBuilder queryBuilder = new StringBuilder(baseQuery);
        if (baseQuery.equals(COUNT_QUERY)) {
            LOG.debug("Executing cluster list query: [{}]", queryBuilder.toString());
            return entityManager.createQuery(queryBuilder.toString());
        }
        queryBuilder.append(" ORDER BY ");
        queryBuilder.append("b." + ClusterFilterBy.getFilterField(orderBy));
        queryBuilder.append(" ").append(sortBy);
        Query query = entityManager.createQuery(queryBuilder.toString());
        query.setFirstResult(offset);
        query.setMaxResults(limitBy);
        LOG.debug("Executing cluster list query: [{}]", queryBuilder.toString());
        return query;
    }
}
