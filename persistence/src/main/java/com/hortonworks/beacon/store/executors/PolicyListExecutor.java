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

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyPropertiesBean;
import com.hortonworks.beacon.util.StringFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Beacon store executor for policy listing.
 */
public class PolicyListExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyListExecutor.class);
    private static final String BASE_QUERY = "select OBJECT(b) from PolicyBean b where b.retirementTime IS NULL";
    private static final String COUNT_QUERY = "select count(b.id) from PolicyBean b where b.retirementTime IS NULL ";

    /**
     * Filter by these Fields is supported by RestAPI.
     */
    private enum PolicyFilterByField {
        SOURCECLUSTER("sourceCluster"),
        TARGETCLUSTER("targetCluster"),
        NAME("name"),
        TYPE("type"),
        STATUS("status");

        private String filterType;

        PolicyFilterByField(String filterType) {
            this.filterType = filterType;
        }

        private static String getFilterType(String name) throws IllegalArgumentException {
            try {
                return valueOf(name).filterType;
            } catch (IllegalArgumentException e) {
                LOG.error(e.getMessage(), e);
                throw new IllegalArgumentException(
                    StringFormat.format("Invalid filter type provided. Input filter type: {}", name));
            }
        }
    }

    /**
     * Order by these Fields is supported by REST API.
     */
    private enum PolicyOrderByField {
        SOURCECLUSTER("sourceCluster"),
        TARGETCLUSTER("targetCluster"),
        NAME("name"),
        TYPE("type"),
        STATUS("status"),
        ENDTIME("endTime"),
        STARTTIME("startTime"),
        CREATIONTIME("creationTime"),
        FREQUENCY("frequencyInSec");

        private String orderType;

        PolicyOrderByField(String filterType) {
            this.orderType = filterType;
        }
    }

    public List<PolicyBean> getFilteredPolicy(String filterBy, String orderBy,
                                              String sortOrder, Integer offset, Integer resultsPerPage) {
        Map<String, List<String>> filterMap = parseFilterBy(filterBy);
        Query filterQuery = createFilterQuery(filterMap, orderBy, sortOrder, offset,
                resultsPerPage, BASE_QUERY);
        List resultList = filterQuery.getResultList();
        List<PolicyBean> beanList = new ArrayList<>();
        for (Object result : resultList) {
            PolicyBean policyBean = (PolicyBean) result;
            PolicyPropertiesExecutor executor = new PolicyPropertiesExecutor(policyBean.getId());
            List<PolicyPropertiesBean> policyProperties = executor.getPolicyProperties();
            policyBean.setCustomProperties(policyProperties);
            beanList.add(policyBean);
        }
        return beanList;
    }

    private Query createFilterQuery(Map<String, List<String>> filterMap,
                                    String orderBy, String sortBy, Integer offset, Integer limitBy, String baseQuery) {
        List<String> paramNames = new ArrayList<>();
        List<Object> paramValues = new ArrayList<>();
        int index = 1;
        StringBuilder queryBuilder = new StringBuilder(baseQuery);
        for (Map.Entry<String, List<String>> filter : filterMap.entrySet()) {
            String field = PolicyFilterByField.getFilterType(filter.getKey().toUpperCase());
            StringBuilder fieldBuilder = new StringBuilder("( ");
            for (String value : filter.getValue()) {
                if (fieldBuilder.length() > 2) {
                    fieldBuilder.append(OR);
                }
                fieldBuilder.append("b." + field).
                        append(EQUAL).
                        append(":" + field + index);
                paramNames.add(field + index);
                paramValues.add(value);
                index++;
            }
            if (fieldBuilder.length() > 2) {
                fieldBuilder.append(" )");
                queryBuilder.append(AND);
                queryBuilder.append(fieldBuilder);
            }
        }
        if (!baseQuery.equalsIgnoreCase(COUNT_QUERY)){
            queryBuilder.append(" ORDER BY ");
            queryBuilder.append("b." + PolicyOrderByField.valueOf(orderBy.toUpperCase()).orderType);
            queryBuilder.append(" ").append(sortBy);
        }
        EntityManager entityManager = RequestContext.get().getEntityManager();
        Query query = entityManager.createQuery(queryBuilder.toString());
        query.setFirstResult(offset);
        query.setMaxResults(limitBy);
        for (int i = 0; i < paramNames.size(); i++) {
            query.setParameter(paramNames.get(i), paramValues.get(i));
        }
        LOG.debug("Executing query: [{}]", queryBuilder.toString());
        return query;
    }

    public long getFilteredPolicyCount(String filterBy, String orderBy, String sortOrder, Integer resultsPerPage) {
        Map<String, List<String>> filterMap = parseFilterBy(filterBy);
        Query filterQuery = createFilterQuery(filterMap, orderBy, sortOrder, 0, resultsPerPage, COUNT_QUERY);
        return (long) filterQuery.getSingleResult();
    }
}
