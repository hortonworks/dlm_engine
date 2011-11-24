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
import com.hortonworks.beacon.client.util.EntityHelper;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.ClusterProperties;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.KnoxTokenUtils;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * Builder class to construct Beacon Cluster resource.
 */
public final class ClusterBuilder {

    private ClusterBuilder() {
    }

    public static Cluster buildCluster(final PropertiesIgnoreCase requestProperties,
                                       final String clusterName) throws BeaconException {
        requestProperties.put(ClusterProperties.NAME.getName(), clusterName);
        for (ClusterProperties property : ClusterProperties.values()) {
            if (requestProperties.getPropertyIgnoreCase(property.getName()) == null && property.isRequired()) {
                throw new BeaconException("Missing parameter: {}", property.getName());
            }
        }
        return buildCluster(requestProperties);
    }

    public static Cluster buildCluster(PropertiesIgnoreCase requestProperties) throws BeaconException {
        String name = requestProperties.getPropertyIgnoreCase(ClusterProperties.NAME.getName());
        String description = requestProperties.getPropertyIgnoreCase(ClusterProperties.DESCRIPTION.getName());
        String fsEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.FS_ENDPOINT.getName());
        String beaconEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.BEACON_ENDPOINT.getName());

        String atlasEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.ATLAS_ENDPOINT.getName());
        String rangerEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.RANGER_ENDPOINT.getName());
        String hsEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.HS_ENDPOINT.getName());
        String localCluster = requestProperties.getPropertyIgnoreCase(ClusterProperties.LOCAL.getName());
        boolean isLocal = StringUtils.isNotBlank(localCluster) && Boolean.parseBoolean(localCluster);
        String peers = requestProperties.getPropertyIgnoreCase(ClusterProperties.PEERS.getName());
        String tags = requestProperties.getPropertyIgnoreCase(ClusterProperties.TAGS.getName());
        if (requestProperties.containsKey(BeaconConstants.DFS_NAMESERVICES)) {
            String haFailOverKey = BeaconConstants.DFS_CLIENT_FAILOVER_PROXY_PROVIDER + BeaconConstants.DOT_SEPARATOR
                    + requestProperties.getProperty(BeaconConstants.DFS_NAMESERVICES);
            if (!requestProperties.containsKey(haFailOverKey)) {
                requestProperties.put(haFailOverKey, BeaconConstants.DFS_CLIENT_DEFAULT_FAILOVER_STRATEGY);
            }
        }
        String hiveWarehouse = requestProperties.getProperty(Cluster.ClusterFields.HIVE_WAREHOUSE.getName());
        if (StringUtils.isNotEmpty(hiveWarehouse)) {
            try {
                boolean cloudDataLake = PolicyHelper.isDatasetHCFS(hiveWarehouse);
                requestProperties.put(Cluster.ClusterFields.CLOUDDATALAKE.getName(), String.valueOf(cloudDataLake));
            } catch (BeaconException e) {
                throw new BeaconException("Hive warehouse directory value might not be correct. ", e);
            }
        }
        String hiveCloudEncAlgoName = Cluster.ClusterFields.HIVE_CLOUD_ENCRYPTION_ALGORITHM.getName();
        String hiveCloudEncryptionAlgorithm = requestProperties.getPropertyIgnoreCase(hiveCloudEncAlgoName);
        if (StringUtils.isNotBlank(hiveCloudEncryptionAlgorithm)) {
            EncryptionAlgorithmType hiveClusterEncType = EncryptionAlgorithmType.getEncryptionAlgorithmType(
                    hiveCloudEncryptionAlgorithm);
            if (EncryptionAlgorithmType.NONE.equals(hiveClusterEncType)) {
                throw new BeaconException("Cloud encryption algorithm NONE is not supported for cluster");
            }
            requestProperties.setProperty(hiveCloudEncAlgoName, hiveClusterEncType.name());
        }
        Properties properties = EntityHelper.getCustomProperties(requestProperties,
                ClusterProperties.getClusterElements());
        String user = requestProperties.getPropertyIgnoreCase(ClusterProperties.USER.getName());

        if (BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled()) {
            String knoxGatewayURL = properties.getProperty(KnoxTokenUtils.KNOX_GATEWAY_URL);
            if (StringUtils.isEmpty(knoxGatewayURL)) {
                throw new BeaconException("Cluster entities submitted must have knox gateway"
                        + " url in knox proxy enabled clusters");
            }
            String fixedGatewayURL = KnoxTokenUtils.getFixedKnoxURL(knoxGatewayURL);

            properties.setProperty(KnoxTokenUtils.KNOX_GATEWAY_URL, fixedGatewayURL);
        }

        return new Cluster.Builder(name, description, beaconEndpoint)
                .fsEndpoint(fsEndpoint).hsEndpoint(hsEndpoint).atlasEndpoint(atlasEndpoint)
                .rangerEndpoint(rangerEndpoint).tags(tags).peers(peers).customProperties(properties)
                .user(user).local(isLocal).build();
    }
}
