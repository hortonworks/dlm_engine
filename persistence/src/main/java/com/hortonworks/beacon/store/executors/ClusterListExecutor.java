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

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreService;
import com.hortonworks.beacon.store.bean.ClusterBean;
import javax.persistence.Query;
import java.util.List;

/**
 * Beacon store executor for Cluster listing.
 */
public class ClusterListExecutor {

    private BeaconStoreService store = Services.get().getService(BeaconStoreService.SERVICE_NAME);

    private static final BeaconLog LOG = BeaconLog.getLog(ClusterListExecutor.class);
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
                        ResourceBundleService.getService().getString(MessageCode.PERS_000005.name(), name));
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
            LOG.info(MessageCode.PERS_000016.name(), queryBuilder.toString());
            return store.getEntityManager().createQuery(queryBuilder.toString());
        }
        queryBuilder.append(" ORDER BY ");
        queryBuilder.append("b." + ClusterFilterBy.getFilterField(orderBy));
        queryBuilder.append(" ").append(sortBy);
        Query query = store.getEntityManager().createQuery(queryBuilder.toString());
        query.setFirstResult(offset);
        query.setMaxResults(limitBy);
        LOG.info(MessageCode.PERS_000016.name(), queryBuilder.toString());
        return query;
    }
}
