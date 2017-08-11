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

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.store.bean.PolicyBean;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Beacon store executor for policy listing.
 */
public class PolicyListExecutor extends BaseExecutor {

    private static final BeaconLog LOG = BeaconLog.getLog(PolicyListExecutor.class);
    private static final String BASE_QUERY = "select OBJECT(b) from PolicyBean b where b.retirementTime IS NULL";
    private static final String AND = " AND ";
    private static final String OR = " OR ";
    private static final String EQUAL = " = ";
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
                        ResourceBundleService.getService().getString(MessageCode.PERS_000005.name(), name));
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
        EntityManager entityManager = null;
        try {
            Map<String, List<String>> filterMap = parseFilters(filterBy);
            entityManager = STORE.getEntityManager();
            Query filterQuery = createFilterQuery(filterMap, orderBy, sortOrder, offset,
                    resultsPerPage, BASE_QUERY, entityManager);
            List resultList = filterQuery.getResultList();
            List<PolicyBean> beanList = new ArrayList<>();
            for (Object result : resultList) {
                beanList.add((PolicyBean) result);
            }
            return beanList;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    private Query createFilterQuery(Map<String, List<String>> filterMap,
                                    String orderBy, String sortBy, Integer offset, Integer limitBy, String baseQuery,
                                    EntityManager entityManager) {
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

        Query query = entityManager.createQuery(queryBuilder.toString());
        query.setFirstResult(offset - 1);
        query.setMaxResults(limitBy);
        for (int i = 0; i < paramNames.size(); i++) {
            query.setParameter(paramNames.get(i), paramValues.get(i));
        }
        LOG.info(MessageCode.PERS_000025.name(), queryBuilder.toString());
        return query;
    }

    static Map<String, List<String>> parseFilters(String filterBy) {
        // Filter the results by specific field:value, eliminate empty values
        Map<String, List<String>> filterByFieldValues = new HashMap<>();
        if (StringUtils.isNotEmpty(filterBy)) {
            String[] fieldValueArray = filterBy.split(BeaconConstants.COMMA_SEPARATOR);
            for (String fieldValue : fieldValueArray) {
                String[] splits = fieldValue.split(BeaconConstants.COLON_SEPARATOR, 2);
                String filterByField = splits[0];
                if (splits.length == 2 && !splits[1].equals("")) {
                    List<String> currentValue = filterByFieldValues.get(filterByField);
                    if (currentValue == null) {
                        currentValue = new ArrayList<>();
                        filterByFieldValues.put(filterByField, currentValue);
                    }

                    String[] fields = splits[1].split("\\|");
                    for (String field : fields) {
                        currentValue.add(field);
                    }
                }
            }
        }
        return filterByFieldValues;
    }

    public long getFilteredPolicyCount(String filterBy, String orderBy, String sortOrder, Integer resultsPerPage) {
        EntityManager entityManager = null;
        try {
            Map<String, List<String>> filterMap = parseFilters(filterBy);
            entityManager = STORE.getEntityManager();
            Query filterQuery = createFilterQuery(filterMap, orderBy, sortOrder, 1,
                    resultsPerPage, COUNT_QUERY, entityManager);
            return (long) filterQuery.getSingleResult();
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }
}
