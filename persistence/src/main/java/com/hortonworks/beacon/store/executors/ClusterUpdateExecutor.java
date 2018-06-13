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

import com.hortonworks.beacon.store.bean.ClusterBean;
import com.hortonworks.beacon.store.bean.ClusterPropertiesBean;
import com.hortonworks.beacon.store.executors.ClusterPropertiesExecutor.ClusterPropertiesQuery;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Query;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * Beacon store executor for Cluster entity update.
 */
public class ClusterUpdateExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterUpdateExecutor.class);

    private static final String UPDATE_CLUSTER = "update ClusterBean b set ";

    public ClusterUpdateExecutor() {
    }

    public void persistUpdatedCluster(ClusterBean updatedCluster, PropertiesIgnoreCase updatedProps,
                                      PropertiesIgnoreCase newProps) {
        Query clusterUpdateQuery = createClusterUpdateQuery(updatedCluster);
        List<Query> clusterPropUpdateQuery = createClusterPropUpdateQuery(updatedCluster, updatedProps);
        List<ClusterPropertiesBean> clusterPropertiesBeans = insertNewClusterProps(updatedCluster, newProps);
        clusterUpdateQuery.executeUpdate();
        for (Query query : clusterPropUpdateQuery) {
            query.executeUpdate();
        }
        for (ClusterPropertiesBean bean : clusterPropertiesBeans) {
            getEntityManager().persist(bean);
        }
    }

    private List<ClusterPropertiesBean> insertNewClusterProps(ClusterBean updatedCluster,
                                                              PropertiesIgnoreCase newProps) {
        List<ClusterPropertiesBean> propBeans = new ArrayList<>();
        Date createdTime = new Date();
        for (String property : newProps.stringPropertyNames()) {
            ClusterPropertiesBean propBean = new ClusterPropertiesBean();
            propBean.setClusterName(updatedCluster.getName());
            propBean.setClusterVersion(updatedCluster.getVersion());
            propBean.setName(property);
            propBean.setValue(newProps.getPropertyIgnoreCase(property));
            propBean.setCreatedTime(createdTime);
            propBeans.add(propBean);
            LOG.debug("Cluster new custom properties key: [{}], value: [{}]",
                    property, newProps.getPropertyIgnoreCase(property));
        }
        return propBeans;
    }

    private List<Query> createClusterPropUpdateQuery(ClusterBean updatedCluster, PropertiesIgnoreCase updatedProps) {
        List<Query> clusterPropUpdateQueries = new ArrayList<>();
        for (String property : updatedProps.stringPropertyNames()) {
            Query query = getEntityManager().createNamedQuery(ClusterPropertiesQuery.UPDATE_CLUSTER_PROP.name());
            LOG.debug("Cluster custom properties update query: [{}]", query.toString());
            query.setParameter("valueParam", updatedProps.getPropertyIgnoreCase(property));
            query.setParameter("clusterNameParam", updatedCluster.getName());
            query.setParameter("clusterVersionParam", updatedCluster.getVersion());
            query.setParameter("nameParam", property);
            clusterPropUpdateQueries.add(query);
        }
        return clusterPropUpdateQueries;
    }

    private Query createClusterUpdateQuery(ClusterBean updatedCluster) {
        StringBuilder queryBuilder = new StringBuilder();
        int index = 1;
        List<String> paramNames = new ArrayList<>();
        List<Object> paramValues = new ArrayList<>();
        String key;

        queryBuilder.append("b.changeId = b.changeId+1");

        if (StringUtils.isNotBlank(updatedCluster.getDescription())) {
            key = "description" + index++;
            queryBuilder.append(", b.description = :").append(key);
            paramNames.add(key);
            paramValues.add(updatedCluster.getDescription());
        }

        if (StringUtils.isNotBlank(updatedCluster.getFsEndpoint())) {
            key = "fsEndpoint" + index++;
            queryBuilder.append(", b.fsEndpoint = :").append(key);
            paramNames.add(key);
            paramValues.add(updatedCluster.getFsEndpoint());
        }

        if (StringUtils.isNotBlank(updatedCluster.getHsEndpoint())) {
            key = "hsEndpoint" + index++;
            queryBuilder.append(", b.hsEndpoint = :").append(key);
            paramNames.add(key);
            paramValues.add(updatedCluster.getHsEndpoint());
        }

        if (StringUtils.isNotBlank(updatedCluster.getRangerEndpoint())) {
            key = "rangerEndpoint" + index++;
            queryBuilder.append(", b.rangerEndpoint = :").append(key);
            paramNames.add(key);
            paramValues.add(updatedCluster.getRangerEndpoint());
        }

        // TODO : update for Atlas and Beacon end point are pending.

        if (StringUtils.isNotBlank(updatedCluster.getTags())) {
            key = "tags" + index++;
            queryBuilder.append(", b.tags = :").append(key);
            paramNames.add(key);
            paramValues.add(updatedCluster.getTags());
        }

        // where clause
        key = "name" + index++;
        queryBuilder.append(" where b.name = :").append(key);
        paramNames.add(key);
        paramValues.add(updatedCluster.getName());

        key = "version" + index++;
        queryBuilder.append(" AND b.version = :").append(key);
        paramNames.add(key);
        paramValues.add(updatedCluster.getVersion());

        queryBuilder.append(" AND b.retirementTime IS NULL");

        String query = UPDATE_CLUSTER + queryBuilder.toString();
        LOG.debug("Cluster update query: [{}]", query);
        Query updateQuery = getEntityManager().createQuery(query);
        for (int i = 0; i < index-1; i++) {
            updateQuery.setParameter(paramNames.get(i), paramValues.get(i));
        }
        return updateQuery;
    }
}
