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

import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.CloudCred.Provider;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.store.bean.CloudCredBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Query;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Beacon store executor for cloud cred entity listing.
 */
public class CloudCredListExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(CloudCredListExecutor.class);
    private static final String BASE_QUERY = "select OBJECT(b) from CloudCredBean b ";
    private static final String COUNT_QUERY = "select count(b.id) from CloudCredBean b ";

    /**
     * Supported filter fields for cloud cred list API.
     */
    public enum CloudCredFields {
        NAME("name"),
        PROVIDER("provider"),
        AUTHTYPE("authtype");

        private final String filterType;

        CloudCredFields(String filterType) {
            this.filterType = filterType;
        }

        public static String getFilterType(String field) throws BeaconException {
            try {
                CloudCredFields valueOf = valueOf(field.toUpperCase());
                return valueOf.filterType;
            } catch (IllegalArgumentException e) {
                throw new BeaconException("Invalid filter type provided. Input filter type: " + field);
            }
        }
    }

    public List<CloudCredBean> listCloudCred(String filterBy, String orderBy, String sortOrder,
                                             Integer offset, Integer resultsPerPage) throws BeaconException {
        Map<String, List<String>> filterByMap = parseFilterBy(filterBy);
        Query query = createQuery(filterByMap, orderBy, sortOrder, offset, resultsPerPage, BASE_QUERY);
        List resultList = query.getResultList();
        List<CloudCredBean> beans = new ArrayList<>();
        for (Object result : resultList) {
            beans.add((CloudCredBean)result);
        }
        return beans;
    }

    public long countListCloudCred(String filterBy) throws BeaconException {
        Map<String, List<String>> filterByMap = parseFilterBy(filterBy);
        Query query = createQuery(filterByMap, null, null, 0, 1, COUNT_QUERY);
        return (long) query.getSingleResult();
    }

    private Query createQuery(Map<String, List<String>> filterByMap, String orderBy, String sortOrder, Integer offset,
                              Integer resultsPerPage, String baseQuery) throws BeaconException {
        List<String> paramNames = new ArrayList<>();
        List<Object> paramValues = new ArrayList<>();
        int index = 1;
        StringBuilder filterBuilder = null;
        for (Map.Entry<String, List<String>> entry : filterByMap.entrySet()) {
            String field = CloudCredFields.getFilterType(entry.getKey());
            StringBuilder builder = null;
            for (String value : entry.getValue()) {
                builder = builder == null ? new StringBuilder("( ") : builder.append(OR);
                builder.append("b." + field).append(EQUAL).append(":" + field + index);
                paramNames.add(field + index++);
                paramValues.add(value);
            }
            if (builder != null && builder.length() > 2) {
                builder.append(" )");
                filterBuilder = filterBuilder == null ? new StringBuilder("where ") : filterBuilder.append(AND);
                filterBuilder.append(builder);
            }
        }
        StringBuilder queryBuilder = new StringBuilder(baseQuery);
        queryBuilder = filterBuilder == null ? queryBuilder : queryBuilder.append(filterBuilder);
        if (!baseQuery.equals(COUNT_QUERY)) {
            queryBuilder.append(" ORDER BY ");
            queryBuilder.append("b." + CloudCredFields.getFilterType(orderBy));
            queryBuilder.append(" " + sortOrder);
        }
        Query query = getEntityManager().createQuery(queryBuilder.toString());
        query.setFirstResult(offset);
        query.setMaxResults(resultsPerPage);
        for (int i = 0; i < paramNames.size(); i++) {
            if (paramNames.get(i).startsWith(CloudCredFields.PROVIDER.filterType)) {
                Provider value = Provider.valueOf((String) paramValues.get(i));
                LOG.debug("Logging the value for provider: {}", value);
                query.setParameter(paramNames.get(i), value);
            } else if (paramNames.get(i).startsWith(CloudCredFields.AUTHTYPE.filterType)) {
                CloudCred.AuthType value = CloudCred.AuthType.valueOf((String) paramValues.get(i));
                LOG.debug("Logging the value for authType: {}", value);
                query.setParameter(paramNames.get(i), value);
            } else {
                query.setParameter(paramNames.get(i), paramValues.get(i));
            }
        }
        LOG.debug("Executing query: [{}]", queryBuilder.toString());
        return query;
    }
}
