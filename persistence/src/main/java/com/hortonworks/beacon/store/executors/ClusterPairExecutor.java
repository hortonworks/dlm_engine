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

import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.ClusterPairBean;
import com.hortonworks.beacon.util.StringFormat;

import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Beacon store executor for ClusterPair.
 */
public class ClusterPairExecutor extends BaseExecutor {

    /**
     * Enum for ClusterPair named queries.
     */
    public enum ClusterPairQuery {
        GET_CLUSTER_PAIR,
        UPDATE_CLUSTER_PAIR_STATUS,
        EXIST_CLUSTER_PAIR
    }

    private static final Logger LOG = LoggerFactory.getLogger(ClusterPairExecutor.class);

    private ClusterPairBean bean;

    public ClusterPairExecutor(ClusterPairBean bean) {
        this.bean = bean;
    }


    private Query getQuery(ClusterPairQuery namedQuery) {
        Query query = getEntityManager().createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_CLUSTER_PAIR:
                query.setParameter("clusterName", bean.getClusterName());
                query.setParameter("clusterVersion", bean.getClusterVersion());
                query.setParameter("pairedClusterName", bean.getClusterName());
                query.setParameter("pairedClusterVersion", bean.getClusterVersion());
                break;
            case UPDATE_CLUSTER_PAIR_STATUS:
                query.setParameter("clusterName", bean.getClusterName());
                query.setParameter("clusterVersion", bean.getClusterVersion());
                query.setParameter("pairedClusterName", bean.getPairedClusterName());
                query.setParameter("pairedClusterVersion", bean.getPairedClusterVersion());
                query.setParameter("status", bean.getStatus());
                query.setParameter("lastModifiedTime", bean.getLastModifiedTime());
                break;
            case EXIST_CLUSTER_PAIR:
                query.setParameter("clusterName", bean.getClusterName());
                query.setParameter("clusterVersion", bean.getClusterVersion());
                query.setParameter("pairedClusterName", bean.getPairedClusterName());
                query.setParameter("pairedClusterVersion", bean.getPairedClusterVersion());
                break;
            default:
                throw new IllegalArgumentException(
                    StringFormat.format("Invalid named query parameter passed: {}", namedQuery.name()));
        }
        return query;
    }


    public List<ClusterPairBean> getPairedCluster() {
        Query query = getQuery(ClusterPairQuery.GET_CLUSTER_PAIR);
        LOG.debug("Executing query {}", query);
        List<ClusterPairBean> resultList = query.getResultList();
        if (resultList == null || resultList.isEmpty()) {
            LOG.info("No pairing data found. Cluster name: [{}], version: [{}]",
                    bean.getClusterName(), bean.getClusterVersion());
            resultList = new ArrayList<>();
        }
        return resultList;
    }

    public void updateStatus() throws BeaconStoreException {
        Query query = getQuery(ClusterPairQuery.UPDATE_CLUSTER_PAIR_STATUS);
        int executeUpdate = query.executeUpdate();
        LOG.info("Cluster [local: {}, remote: {}] pair status: [{}] updated for [{}] records.",
                bean.getClusterName(), bean.getPairedClusterName(), executeUpdate, bean.getStatus());
    }

    public void pairCluster() throws BeaconStoreException {
        Query query = getQuery(ClusterPairQuery.EXIST_CLUSTER_PAIR);
        List<ClusterPairBean> resultList =  query.getResultList();
        if (resultList == null || resultList.isEmpty()) {
            LOG.debug("Storing cluster pair data. Source Cluster [{}, {}], Remote Cluster [{}, {}]",
                    bean.getClusterName(), bean.getClusterVersion(),
                    bean.getPairedClusterName(), bean.getPairedClusterVersion());
            getEntityManager().persist(bean);
            LOG.info("Cluster pair data stored. Source Cluster [{}, {}], Remote Cluster [{}, {}]",
                    bean.getClusterName(), bean.getClusterVersion(),
                    bean.getPairedClusterName(), bean.getPairedClusterVersion());
        } else if (resultList.size() == 1) {
            updateStatus();
        } else {
            LOG.warn("ClusterPair table is in inconsistent state. Number of records found: {}", resultList.size());
            throw new IllegalStateException(StringFormat.format(
                    "ClusterPair table is in inconsistent state. Number of records found: {}", resultList.size()));
        }
    }
}
