/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

/**
 * Helper util class for Beacon ReplicationPolicy resource.
 */
public final class PolicyHelper {

    private PolicyHelper() {
    }

    public static String getRemoteBeaconEndpoint(final ReplicationPolicy policy) throws BeaconException {

        if (PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            throw new BeaconException(MessageCode.ENTI_000001.name(), policy.getName());
        }
        String remoteClusterName = getRemoteClusterName(policy);
        Cluster remoteCluster = ClusterHelper.getActiveCluster(remoteClusterName);
        return remoteCluster.getBeaconEndpoint();
    }

    public static String getRemoteClusterName(final ReplicationPolicy policy) throws BeaconException {
        String localClusterName = ClusterHelper.getLocalCluster().getName();
        return localClusterName.equalsIgnoreCase(policy.getSourceCluster())
                ? policy.getTargetCluster() : policy.getSourceCluster();
    }

    public static boolean isPolicyHCFS(final String sourceDataset, final String targetDataset) throws BeaconException {
        if (StringUtils.isNotBlank(sourceDataset)) {
            Path sourceDatasetPath = new Path(sourceDataset);
            if (FSUtils.isHCFS(sourceDatasetPath)) {
                return true;
            }
        }

        if (StringUtils.isNotBlank(targetDataset)) {
            Path targetDatasetPath = new Path(targetDataset);
            if (FSUtils.isHCFS(targetDatasetPath)) {
                return true;
            }
        }
        return false;
    }


    public static String getRMTokenConf() {
        StringBuilder rmTokenConf = new StringBuilder();
        rmTokenConf.append("dfs.nameservices|")
                .append("^dfs.namenode.rpc-address.*$|")
                .append("^dfs.ha.namenodes.*$|")
                .append("^dfs.client.failover.proxy.provider.*$|")
                .append("dfs.namenode.kerberos.principal");
        return rmTokenConf.toString();
    }
}
