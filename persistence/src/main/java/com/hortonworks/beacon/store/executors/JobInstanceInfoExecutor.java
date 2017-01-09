/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.replication.ReplicationType;
import com.hortonworks.beacon.store.BeaconStore;
import com.hortonworks.beacon.store.bean.JobInstanceBean;
import com.hortonworks.beacon.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobInstanceInfoExecutor {

    private static final String AND = " AND ";
    private static final Logger LOG = LoggerFactory.getLogger(JobInstanceInfoExecutor.class);

    enum Filters {

        NAME("name", " = ", false),
        STATUS("status", " = ", false),
        TYPE("type", " = ", true),
        START_TIME("startTime", " >= ", true),
        END_TIME("endTime", " <= ", true);

        String filterType;
        String operation;
        boolean isParse;

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

    public List<JobInstanceBean> getFilteredJobInstance(String filter, String orderBy, String sortBy,
                                                        Integer offset, Integer limitBy) throws Exception {
        Map<String, String> filterMap = parseFilters(filter);
        Query filterQuery = createFilterQuery(filterMap, orderBy, sortBy, offset, limitBy);
        List resultList = filterQuery.getResultList();
        List<JobInstanceBean> beanList = new ArrayList<>();
        for (Object result : resultList) {
            beanList.add((JobInstanceBean) result);
        }
        return beanList;
    }

    private Map<String, String> parseFilters(String filters) {
        Map<String, String> filterMap = new HashMap<>();
        String filterArray[] = filters.split(";");
        if (filterArray.length > 0) {
            for (String pair : filterArray) {
                String keyValue[] = pair.split("=");
                if (keyValue.length != 2) {
                    throw new IllegalArgumentException("Invalid filter key=value pair provided: " + keyValue[0] + "=" +
                            keyValue[1]);
                }
                Filters.getFilter(keyValue[0]);
                filterMap.put(keyValue[0], keyValue[1]);
            }
        } else {
            throw new IllegalArgumentException("Invalid filters provided: " + filters);
        }
        return filterMap;
    }

    private Query createFilterQuery(Map<String, String> filterMap,
                                    String orderBy, String sortBy, Integer offset, Integer limitBy) {
        List<String> paramNames = new ArrayList<>();
        List<Object> paramValues = new ArrayList<>();
        int index = 1;
        StringBuilder queryBuilder = new StringBuilder("SELECT OBJECT(b) FROM JobInstanceBean b WHERE ");
        boolean appendAnd = false;
        for (Map.Entry<String, String> filter : filterMap.entrySet()) {
            if (appendAnd) {
                queryBuilder.append(AND);
            }
            Filters fieldFilter = Filters.getFilter(filter.getKey());
            queryBuilder.append("b." + fieldFilter.getFilterType()).
                    append(fieldFilter.getOperation()).
                    append(":" + fieldFilter.getFilterType() + index);
            paramNames.add(fieldFilter.getFilterType() + index);
            paramValues.add(getParsedValue(fieldFilter, filter.getValue()));
            index++;
            appendAnd = true;
        }
        queryBuilder.append(" ORDER BY ");
        queryBuilder.append("b." + Filters.getFilter(orderBy).getFilterType());
        queryBuilder.append(" ").append(sortBy);

        Query query = BeaconStore.getInstance().getEntityManager().createQuery(queryBuilder.toString());
        query.setFirstResult(offset - 1);
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
                return ReplicationType.valueOf(value.toUpperCase()).getName();
            default:
                throw new IllegalArgumentException("Parsing implementation is not present for filter: " +
                        fieldFilter.getFilterType());
        }
    }
}