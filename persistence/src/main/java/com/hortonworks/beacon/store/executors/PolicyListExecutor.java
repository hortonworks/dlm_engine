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
