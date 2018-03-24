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

package com.hortonworks.beacon.tools;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.BeaconWebClient;
import com.hortonworks.beacon.client.entity.Cluster;

/**
 * Beacon cluster update utility.
 * Utility performs all the operations on the target cluster.
 */
public class BeaconClusterUpdate {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconClusterUpdate.class);
    private static BeaconWebClient targetClient;
    private static BeaconWebClient sourceClient;
    private static BeaconClusterUpdate clusterUpdate;

    private static ClusterUpdateDefinition definition;


    private void init(String filePath) throws Exception {
        File definitionFile = new File(filePath);
        try(FileReader reader = new FileReader(definitionFile)) {
            Yaml yaml = new Yaml();
            definition = yaml.loadAs(reader, ClusterUpdateDefinition.class);
        } catch (Exception e) {
            throw new Exception(e);
        }

        String sourceBeaconEndPoint = definition.getSourceBeaconEndPoint();
        String targetBeaconEndPoint = definition.getTargetBeaconEndPoint();
        System.out.println("sourceBeaconEndPoint:"+sourceBeaconEndPoint);
        System.out.println("targetBeaconEndPoint:"+targetBeaconEndPoint);
        validateBeaconEndPoint(sourceBeaconEndPoint);
        validateBeaconEndPoint(targetBeaconEndPoint);
        targetClient = new BeaconWebClient(targetBeaconEndPoint);
        sourceClient = new BeaconWebClient(sourceBeaconEndPoint);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Input cluster update configuration file is missing.");
            System.exit(1);
        }

        String filePath = args[0];
        if (!new File(filePath).exists() || new File(filePath).length() == 0) {
            System.err.println("Input cluster update configuration file does not exists or empty. File: " + filePath);
            System.exit(2);
        }

        clusterUpdate = new BeaconClusterUpdate();
        clusterUpdate.init(filePath);

        String sourceCluster = definition.getSourceClusterName();
        String targetCluster = definition.getTargetClusterName();

        clusterUpdate.validateInput(definition.getSourceBeaconEndPoint(), "sourceBeaconEndPoint");
        clusterUpdate.validateInput(definition.getTargetBeaconEndPoint(), "targetBeaconEndPoint");

        if (StringUtils.isNotBlank(sourceCluster) && StringUtils.isNotBlank(targetCluster)) {
            // we need to update both source and target cluster.
            LOG.debug("Source: [{}] and Target: [{}] clusters to be updated.", sourceCluster, targetCluster);
            clusterUpdate.updateSourceTargetCluster();
        } else if (StringUtils.isNotBlank(sourceCluster)) {
            // We need to update the source cluster
            List<String> properties = getCombinedProperties(definition.getSourceClusterEndPoints(),
                    definition.getSourceClusterProperties());
            LOG.debug("Source: [{}] will be updated. Update Properties: [{}]", sourceCluster, properties);
            clusterUpdate.updateSourceCluster(definition.getSourceClusterName(),
                    properties);
        } else if (StringUtils.isNotBlank(targetCluster)) {
            // We need to update the target cluster
            List<String> properties = getCombinedProperties(definition.getTargetClusterEndPoints(),
                    definition.getTargetClusterProperties());
            LOG.debug("Target: [{}] will be updated. Update Properties: [{}]", targetCluster, properties);
            clusterUpdate.updateTargetCluster(definition.getTargetClusterName(),
                    properties);
        }
    }

    private static List<String> getCombinedProperties(List<String> clusterEndPoints,
                                                      List<String> clusterProperties) {
        clusterEndPoints = clusterEndPoints == null ? new ArrayList<String>() : clusterEndPoints;
        clusterProperties = clusterProperties == null ? new ArrayList<String>() : clusterProperties;
        List<String> properties = new ArrayList<>();
        properties.addAll(clusterEndPoints);
        properties.addAll(clusterProperties);
        return properties;
    }

    private void updateSourceCluster(String clusterName, List<String> properties) throws BeaconClientException {
        validateInput(properties, "properties");
        performUpdate(clusterName, properties);
    }

    private void updateTargetCluster(String clusterName, List<String> properties) throws BeaconClientException {
        validateInput(properties, "properties");
        performUpdate(clusterName, properties);
    }

    private void performUpdate(String clusterName, List<String> properties)
            throws BeaconClientException {
        // The cluster entity exists on the clusters.
        Cluster targetClusterDefinition = getClusterDefinition(targetClient, clusterName);
        LOG.debug("Before update targetClusterDefinition: {}", targetClusterDefinition);
        Cluster sourceClusterDefinition = getClusterDefinition(sourceClient, clusterName);
        LOG.debug("Before update sourceClusterDefinition: {}", sourceClusterDefinition);

        LOG.debug("Updating cluster definition on target: {}", clusterName);
        updateCluster(targetClient, clusterName, properties);
        LOG.debug("Updating cluster definition on source: {}", clusterName);
        updateCluster(sourceClient, clusterName, properties);

    }

    private void updateSourceTargetCluster() throws BeaconClientException {
        List<String> sourceClusterProperties = getCombinedProperties(definition.getSourceClusterEndPoints(),
                definition.getSourceClusterProperties());
        LOG.debug("Source cluster update properties: [{}]", sourceClusterProperties);

        List<String> targetClusterProperties = getCombinedProperties(definition.getTargetClusterEndPoints(),
                definition.getTargetClusterProperties());
        LOG.debug("Target cluster update properties: [{}]", targetClusterProperties);

        validateInput(sourceClusterProperties, "(sourceClusterEndPoints, sourceClusterProperties)");
        validateInput(targetClusterProperties, "(targetClusterEndPoints, targetClusterProperties(");

        String sourceClusterName = definition.getSourceClusterName();
        String targetClusterName = definition.getTargetClusterName();

        // The cluster entity exists on the clusters. Store the entity into a file.
        Cluster sourceClusterDefinition = getClusterDefinition(targetClient, sourceClusterName);
        LOG.info("Before update sourceClusterDefinition: {}", sourceClusterDefinition);
        Cluster targetClusterDefinition = getClusterDefinition(targetClient, targetClusterName);
        LOG.info("Before update targetClusterDefinition: {}", targetClusterDefinition);

        // update on target cluster.
        updateCluster(targetClient, sourceClusterName, sourceClusterProperties);
        updateCluster(targetClient, targetClusterName, targetClusterProperties);

        // update on source cluster.
        updateCluster(sourceClient, sourceClusterName, sourceClusterProperties);
        updateCluster(sourceClient, targetClusterName, targetClusterProperties);
    }

    private void validateBeaconEndPoint(String beaconEndPoint) throws BeaconClientException {
        BeaconWebClient client = new BeaconWebClient(beaconEndPoint);
        client.getServiceVersion();
    }

    private Cluster getClusterDefinition(BeaconWebClient client, String clusterName) throws BeaconClientException {
        return client.getCluster(clusterName);
    }

    private void updateCluster(BeaconWebClient client, String clusterName, List<String> properties)
            throws BeaconClientException {
        String updateDefinition = StringUtils.join(properties, System.lineSeparator());
        LOG.debug("Updating cluster [{}], beaconServer: [{}], properties: [{}]",
                clusterName, client, properties);
        client.updateCluster(clusterName, updateDefinition);
    }

    private void validateInput(Object object, String name) {
        boolean blank = false;
        if (object == null) {
            blank = true;
        } else if (object instanceof String) {
            blank = StringUtils.isBlank((String) object);
        } else if (object instanceof List) {
            List<String> list = (List<String>) object;
            blank = CollectionUtils.isEmpty(list);
        }
        if (blank) {
            throw new IllegalArgumentException(name + " Cannot be null or empty.");
        }
    }
}
