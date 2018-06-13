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

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.EncryptionAlgorithmType;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper util class for Beacon ReplicationPolicy resource.
 */
public final class PolicyHelper {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyHelper.class);

    private PolicyHelper() {
    }

    public static String getRemoteBeaconEndpoint(final ReplicationPolicy policy) throws BeaconException {

        if (PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            throw new BeaconException("No remote beacon endpoint for HCFS policy: {}", policy.getName());
        }
        String remoteClusterName = getRemoteClusterName(policy);
        Cluster remoteCluster = ClusterHelper.getActiveCluster(remoteClusterName);
        return remoteCluster.getBeaconEndpoint();
    }

    public static String getRemoteKnoxBaseURL(final ReplicationPolicy policy) throws BeaconException {

        if (PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            throw new BeaconException("No remote beacon endpoint for HCFS policy: {}", policy.getName());
        }
        String remoteClusterName = getRemoteClusterName(policy);
        Cluster remoteCluster = ClusterHelper.getActiveCluster(remoteClusterName);
        return remoteCluster.getKnoxGatewayURL();
    }

    public static String getRemoteClusterName(final ReplicationPolicy policy) throws BeaconException {
        String localClusterName = ClusterHelper.getLocalCluster().getName();
        return localClusterName.equalsIgnoreCase(policy.getSourceCluster())
                ? policy.getTargetCluster() : policy.getSourceCluster();
    }

    public static boolean isPolicyHCFS(final ReplicationPolicy policy) throws BeaconException {
        return isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset());
    }

    public static boolean isPolicyHCFS(final String sourceDataset, final String targetDataset) throws BeaconException {
        return isDatasetHCFS(sourceDataset) || isDatasetHCFS(targetDataset);
    }

    public static boolean isDatasetHCFS(final String dataset) throws BeaconException {
        if (StringUtils.isNotBlank(dataset)) {
            Path datasetPath = new Path(dataset);
            if (FSUtils.isHCFS(datasetPath)) {
                return true;
            }
        }
        return false;
    }

    public static String escapeDataSet(String dataset) {
        if (dataset != null && !dataset.startsWith("`") && !dataset.endsWith("`")) {
            dataset = "`" + dataset + "`";
        }
        return dataset;
    }

    public static boolean isCloudEncryptionEnabled(final ReplicationPolicy policy) {
        return StringUtils.isNotEmpty(policy.getCloudEncryptionAlgorithm());
    }

    public static EncryptionAlgorithmType getCloudEncryptionAlgorithm(final ReplicationPolicy policy)
            throws BeaconException {
        if (!isCloudEncryptionEnabled(policy)) {
            return EncryptionAlgorithmType.NONE;
        }
        String cloudEncAlgo = policy.getCloudEncryptionAlgorithm();
        try {
            return EncryptionAlgorithmType.valueOf(cloudEncAlgo);
        } catch (IllegalArgumentException iEx) {
            throw new BeaconException("Invalid cloud algorithm type is specified " + cloudEncAlgo, iEx);
        } catch (NullPointerException npe) {
            throw new BeaconException("Cloud Encryption Algorithm cannot be null", npe);
        }
    }

    public static String getRMTokenConf() {
        StringBuilder rmTokenConf = new StringBuilder();
        rmTokenConf.append("dfs.nameservices|")
                .append("^dfs.namenode.rpc-address.*$|")
                .append("^dfs.ha.namenodes.*$|")
                .append("^dfs.client.failover.proxy.provider.*$|")
                .append("dfs.namenode.kerberos.principal|")
                .append("dfs.namenode.kerberos.principal.pattern|")
                .append("mapreduce.jobhistory.principal|")
                .append("^ssl.client.*$|")
                .append("^hadoop.ssl.*$|")
                .append("hadoop.rpc.protection|")
                .append("^yarn.timeline-service.*$|")
                .append("fs.defaultFS|")
                .append("yarn.http.policy");
        return rmTokenConf.toString();
    }
}
