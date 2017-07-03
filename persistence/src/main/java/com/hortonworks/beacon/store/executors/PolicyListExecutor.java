/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreService;
import com.hortonworks.beacon.store.bean.PolicyBean;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Beacon store executor for policy listing.
 */
public class PolicyListExecutor {

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
        Map<String, List<String>> filterMap = parseFilters(filterBy);
        Query filterQuery = createFilterQuery(filterMap, orderBy, sortOrder, offset, resultsPerPage, BASE_QUERY);
        List resultList = filterQuery.getResultList();
        List<PolicyBean> beanList = new ArrayList<>();
        for (Object result : resultList) {
            beanList.add((PolicyBean) result);
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

        Query query = ((BeaconStoreService) Services.get().getService(BeaconStoreService.SERVICE_NAME))
                .getEntityManager().createQuery(queryBuilder.toString());
        query.setFirstResult(offset - 1);
        query.setMaxResults(limitBy);
        for (int i = 0; i < paramNames.size(); i++) {
            query.setParameter(paramNames.get(i), paramValues.get(i));
        }
        LOG.info("Executing query: [{}]", queryBuilder.toString());
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
        Map<String, List<String>> filterMap = parseFilters(filterBy);
        Query filterQuery = createFilterQuery(filterMap, orderBy, sortOrder, 1, resultsPerPage, COUNT_QUERY);
        return (long) filterQuery.getSingleResult();
    }
}
