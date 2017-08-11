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
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.ReplicationHelper;
import org.apache.commons.lang3.StringUtils;

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

    private static final String AND = " AND ";
    private static final BeaconLog LOG = BeaconLog.getLog(PolicyInstanceListExecutor.class);
    private static final String BASE_QUERY = "SELECT pb.name, pb.type, pb.executionType, pb.user, OBJECT(b) "
            + "FROM PolicyBean pb, PolicyInstanceBean b "
            + "WHERE b.policyId = pb.id";
    private static final String COUNT_QUERY = "SELECT count(pb.name) FROM PolicyBean pb, PolicyInstanceBean b "
                        + "WHERE b.policyId = pb.id";
    private EntityManager entityManager;

    public void initializeEntityManager() {
        entityManager = STORE.getEntityManager();
    }

    public void closeEntityManager() {
        STORE.closeEntityManager(entityManager);
    }

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
            throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.PERS_000005.name(), fieldName));
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
                        throw new IllegalArgumentException(
                                ResourceBundleService.getService().getString(MessageCode.COMM_010013.name(), pair));
                    }
                    Filters.getFilter(keyValue[0]);
                    filterMap.put(keyValue[0], keyValue[1]);
                }
            } else {
                throw new IllegalArgumentException(
                        ResourceBundleService.getService().getString(MessageCode.COMM_010014.name(), filters));
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

        Query query = entityManager.createQuery(queryBuilder.toString());
        query.setFirstResult(offset);
        query.setMaxResults(limitBy);
        for (int i = 0; i < paramNames.size(); i++) {
            query.setParameter(paramNames.get(i), paramValues.get(i));
        }
        LOG.info(MessageCode.PERS_000025.name(), queryBuilder.toString());
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
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.PERS_000012.name(), fieldFilter.getFilterType()));
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
