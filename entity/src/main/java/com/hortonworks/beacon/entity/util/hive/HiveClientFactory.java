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
package com.hortonworks.beacon.entity.util.hive;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive Metadata Client.
 */
public final class HiveClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HiveClientFactory.class);
    private static HiveServerClient hiveServerClient;

    private HiveClientFactory() {
    }

    private static HiveMetadataClient hiveMetadataClient;

    @VisibleForTesting
    public static void setHiveMetadataClient(HiveMetadataClient hiveClient) {
        HiveClientFactory.hiveMetadataClient = hiveClient;
    }

    @VisibleForTesting
    public static void setHiveServerClient(HiveServerClient hiveClient) {
        HiveClientFactory.hiveServerClient = hiveClient;
    }

    public static HiveServerClient getHiveServerClient(String connectionString) throws BeaconException {
        if (hiveServerClient != null) {
            return hiveServerClient;
        }
        return new HS2Client(connectionString);
    }

    public static HiveServerClient getHiveServerClient(String connectionString,
                                                       Cluster cluster) throws BeaconException {
        if (hiveServerClient != null) {
            return hiveServerClient;
        }
        return new HS2Client(connectionString, cluster);
    }

    public static HiveMetadataClient getMetadataClient(Cluster cluster) throws BeaconException {
        BeaconCluster beaconCluster = new BeaconCluster(cluster);
        if (hiveMetadataClient != null) {
            return hiveMetadataClient;
        }

        if (beaconCluster.getHmsEndpoint() != null) {
            return new HMSMetadataClient(cluster);
        }
        if (cluster.getHsEndpoint() != null) {
            return new HS2Client(cluster);
        }

        throw new BeaconException("Failed to get HiveMetadataClient for cluster {}. Specify HMS/HS2 endpoint",
                cluster.getName());
    }

    public static void close(HiveMetadataClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("Failed to close", e);
            }
        }
    }

    public static void close(HiveServerClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("Failed to close", e);
            }
        }
    }
}
