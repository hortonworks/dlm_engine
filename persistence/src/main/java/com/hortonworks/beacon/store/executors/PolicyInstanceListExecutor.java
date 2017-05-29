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
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreService;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.result.PolicyInstanceList;
import com.hortonworks.beacon.store.result.PolicyInstanceList.InstanceElement;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.ReplicationHelper;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class PolicyInstanceListExecutor {

    private static final String AND = " AND ";
    private static final BeaconLog LOG = BeaconLog.getLog(PolicyInstanceListExecutor.class);
    private static final String BASE_QUERY = "SELECT pb.name, pb.type, pb.executionType, pb.user, OBJECT(b) "
            + "FROM PolicyBean pb, PolicyInstanceBean b "
            + "WHERE b.policyId = pb.id";
    private static final String COUNT_QUERY = "SELECT count(pb.name) FROM PolicyBean pb, PolicyInstanceBean b "
                        + "WHERE b.policyId = pb.id";
    enum Filters {
        NAME("name", " = ", false),
        STATUS("status", " = ", false),
        TYPE("type", " = ", true),
        START_TIME("startTime", " >= ", true),
        END_TIME("endTime", " <= ", true);

        private String filterType;
        private String operation;
        private boolean isParse;

        Filters(String fieldName, String operation, boolean isParse) {
            this.filterType = fieldName;
            this.operation = operation;
            this.isParse = isParse;
        }

        public String getFilterType() {
            return filterType;
        }

        public String getOperation() {
            return operation;
        }

        public boolean isParse() {
            return isParse;
        }

        public static Filters getFilter(String fieldName) {
            for (Filters filter : Filters.values()) {
                if (filter.getFilterType().equalsIgnoreCase(fieldName)) {
                    return filter;
                }
            }
            throw new IllegalArgumentException("Invalid filter type provided. input: " + fieldName);
        }
    }

    public PolicyInstanceList getFilteredJobInstance(String filter, String orderBy, String sortBy,
                              Integer offset, Integer limitBy, boolean isArchived) throws Exception {
        Map<String, String> filterMap = parseFilters(filter);
        List<InstanceElement> elements = new ArrayList<>();
        long totalCount = getFilteredPolicyInstanceCount(filterMap, orderBy, sortBy, limitBy, isArchived);
        if (totalCount > 0) {
            Query filterQuery = createFilterQuery(filterMap, orderBy, sortBy, offset, limitBy, BASE_QUERY, isArchived);
            List<Object[]> resultList = filterQuery.getResultList();
            for (Object[] objects : resultList) {
                String name = (String) objects[0];
                String type = (String) objects[1];
                String executionType = (String) objects[2];
                String user = (String) objects[3];
                PolicyInstanceBean bean = (PolicyInstanceBean) objects[4];
                InstanceElement element = PolicyInstanceList.createInstanceElement(name, type, executionType, user,
                        bean);
                elements.add(element);
            }
        }
        return new PolicyInstanceList(elements, totalCount);
    }

    private Map<String, String> parseFilters(String filters) {
        Map<String, String> filterMap = new HashMap<>();
        if (StringUtils.isNotBlank(filters)) {
            String[] filterArray = filters.split(BeaconConstants.COMMA_SEPARATOR);
            if (filterArray.length > 0) {
                for (String pair : filterArray) {
                    String[] keyValue = pair.split(BeaconConstants.COLON_SEPARATOR, 2);
                    if (keyValue.length != 2) {
                        throw new IllegalArgumentException("Invalid filter key:value pair provided: "
                                + keyValue[0] + ":" + keyValue[1]);
                    }
                    Filters.getFilter(keyValue[0]);
                    filterMap.put(keyValue[0], keyValue[1]);
                }
            } else {
                throw new IllegalArgumentException("Invalid filters provided: " + filters);
            }
        }
        return filterMap;
    }

    private Query createFilterQuery(Map<String, String> filterMap, String orderBy, String sortBy, Integer offset,
                                    Integer limitBy, String baseQuery, boolean isArchived) {
        List<String> paramNames = new ArrayList<>();
        List<Object> paramValues = new ArrayList<>();
        baseQuery = isArchived
                ? baseQuery + " AND b.retirementTime IS NOT NULL AND pb.retirementTime IS NOT NULL "
                : baseQuery + " AND b.retirementTime IS NULL AND pb.retirementTime IS NULL ";
        int index = 1;
        StringBuilder queryBuilder = new StringBuilder(baseQuery);
        for (Map.Entry<String, String> filter : filterMap.entrySet()) {
            queryBuilder.append(AND);
            Filters fieldFilter = Filters.getFilter(filter.getKey());
            if (fieldFilter.equals(Filters.NAME) || fieldFilter.equals(Filters.TYPE)) {
                queryBuilder.append("pb." + fieldFilter.getFilterType()).
                        append(fieldFilter.getOperation()).
                        append(":" + fieldFilter.getFilterType() + index);
            } else {
                queryBuilder.append("b." + fieldFilter.getFilterType()).
                        append(fieldFilter.getOperation()).
                        append(":" + fieldFilter.getFilterType() + index);
            }
            paramNames.add(fieldFilter.getFilterType() + index);
            paramValues.add(getParsedValue(fieldFilter, filter.getValue()));
            index++;
        }
        if (!baseQuery.startsWith(COUNT_QUERY)){
            queryBuilder.append(" ORDER BY ");
            queryBuilder.append("b." + Filters.getFilter(orderBy).getFilterType());
            queryBuilder.append(" ").append(sortBy);
        }

        Query query = ((BeaconStoreService) Services.get().getService(BeaconStoreService.SERVICE_NAME))
                .getEntityManager().createQuery(queryBuilder.toString());
        query.setFirstResult(offset);
        query.setMaxResults(limitBy);
        for (int i = 0; i < paramNames.size(); i++) {
            query.setParameter(paramNames.get(i), paramValues.get(i));
        }
        LOG.info("Executing query: [{}]", queryBuilder.toString());
        return query;
    }

    private Object getParsedValue(Filters fieldFilter, String value) {
        if (!fieldFilter.isParse()) {
            return value;
        }
        switch (fieldFilter) {
            case START_TIME:
            case END_TIME:
                return new java.sql.Timestamp(DateUtil.getDateMillis(value));
            case TYPE:
                return ReplicationHelper.getReplicationType(value).toString();
            default:
                throw new IllegalArgumentException("Parsing implementation is not present for filter: "
                        + fieldFilter.getFilterType());
        }
    }
    private long getFilteredPolicyInstanceCount(Map<String, String> filterMap, String orderBy, String sortBy,
            Integer limitBy, boolean isArchived) {
        Query countQuery = createFilterQuery(filterMap, orderBy, sortBy, 0, limitBy, COUNT_QUERY, isArchived);
        return ((long) countQuery.getSingleResult());
    }
}
