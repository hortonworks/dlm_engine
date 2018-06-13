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

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.store.bean.ClusterBean;
import com.hortonworks.beacon.util.StringFormat;

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
                    StringFormat.format("Invalid filter type provided. Input filter type: {}", name));
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
            return getEntityManager().createQuery(queryBuilder.toString());
        }
        queryBuilder.append(" ORDER BY ");
        queryBuilder.append("b." + ClusterFilterBy.getFilterField(orderBy));
        queryBuilder.append(" ").append(sortBy);
        Query query = getEntityManager().createQuery(queryBuilder.toString());
        query.setFirstResult(offset);
        query.setMaxResults(limitBy);
        LOG.debug("Executing cluster list query: [{}]", queryBuilder.toString());
        return query;
    }
}
