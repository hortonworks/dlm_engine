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
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.ReplicationHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class PolicyInstanceListExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyInstanceListExecutor.class);
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
            throw new IllegalArgumentException("Invalid filter type provided. Input filter type: " + fieldName);
        }
    }

    public List<Object[]> getFilteredJobInstance(String filter, String orderBy, String sortBy,
                                       Integer offset, Integer limitBy, boolean isArchived) throws Exception {
        Map<String, String> filterMap = parseFilters(filter);
        Query filterQuery = createFilterQuery(filterMap, orderBy, sortBy, offset, limitBy, BASE_QUERY, isArchived);
        return (List<Object[]>) filterQuery.getResultList();
    }

    private Map<String, String> parseFilters(String filters) {
        Map<String, String> filterMap = new HashMap<>();
        if (StringUtils.isNotBlank(filters)) {
            String[] filterArray = filters.split(BeaconConstants.COMMA_SEPARATOR);
            if (filterArray.length > 0) {
                for (String pair : filterArray) {
                    String[] keyValue = pair.split(BeaconConstants.COLON_SEPARATOR, 2);
                    if (keyValue.length != 2) {
                        throw new IllegalArgumentException("Invalid filter key:value pair provided: " + pair);
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
                throw new IllegalArgumentException(
                    "Parsing implementation is not present for filter: " + fieldFilter.getFilterType());
        }
    }
    public long getFilteredPolicyInstanceCount(String filter, String orderBy, String sortBy,
                                               Integer limitBy, boolean isArchived) {
        Map<String, String> filterMap = parseFilters(filter);
        Query countQuery = createFilterQuery(filterMap, orderBy, sortBy, 0,
                limitBy, COUNT_QUERY, isArchived);
        return (long) countQuery.getSingleResult();
    }
}
