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

import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.ClusterPairBean;

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
        Query query = entityManager.createNamedQuery(namedQuery.name());
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
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.PERS_000002.name(), namedQuery.name()));
        }
        return query;
    }


    public List<ClusterPairBean> getPairedCluster() {

        Query query = getQuery(ClusterPairQuery.GET_CLUSTER_PAIR);
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
            entityManager.persist(bean);
            LOG.info("Cluster pair data stored. Source Cluster [{}, {}], Remote Cluster [{}, {}]",
                    bean.getClusterName(), bean.getClusterVersion(),
                    bean.getPairedClusterName(), bean.getPairedClusterVersion());
        } else if (resultList.size() == 1) {
            updateStatus();
        } else {
            LOG.warn("ClusterPair table is in inconsistent state. Number of records found: {}", resultList.size());
            throw new IllegalStateException(ResourceBundleService.getService()
                    .getString(MessageCode.PERS_000006.name(), resultList.size()));
        }
    }
}
