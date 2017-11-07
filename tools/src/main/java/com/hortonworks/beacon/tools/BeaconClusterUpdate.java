/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.tools;

import com.hortonworks.beacon.client.BeaconWebClient;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.job.JobStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Beacon cluster update utility.
 * Utility performs all the operations on the target cluster.
 */
public class BeaconClusterUpdate {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconClusterUpdate.class);
    private static BeaconWebClient targetClient;
    private static BeaconWebClient sourceClient;
    private static BeaconClusterUpdate clusterUpdate;

    private static List<String> sourceAbortedPolicies = new ArrayList<>();
    private static List<String> targetAbortedPolicies = new ArrayList<>();
    private static List<String> sourceSuspendedPolicies = new ArrayList<>();
    private static List<String> targetSuspendedPolicies = new ArrayList<>();
    private static boolean abortCalled;
    private static boolean suspendCalled;
    private static boolean isUnpairDone;
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
        validateBeaconEndPoint(sourceBeaconEndPoint);
        validateBeaconEndPoint(targetBeaconEndPoint);
        targetClient = new BeaconWebClient(targetBeaconEndPoint);
        sourceClient = new BeaconWebClient(sourceBeaconEndPoint);
    }

    public static void main(String[] args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
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
            LOG.debug("Source: [{0}] and Target: [{1}] clusters to be updated.", sourceCluster, targetCluster);
            clusterUpdate.updateSourceTargetCluster();
        } else if (StringUtils.isNotBlank(sourceCluster)) {
            // We need to update the source cluster
            List<String> properties = getCombinedProperties(definition.getSourceClusterEndPoints(),
                    definition.getSourceClusterProperties());
            LOG.debug("Source: [{0}] will be updated. Update Properties: [{1}]", sourceCluster, properties);
            clusterUpdate.updateSourceCluster(definition.getSourceClusterName(),
                    properties);
        } else if (StringUtils.isNotBlank(targetCluster)) {
            // We need to update the target cluster
            List<String> properties = getCombinedProperties(definition.getTargetClusterEndPoints(),
                    definition.getTargetClusterProperties());
            LOG.debug("Target: [{0}] will be updated. Update Properties: [{1}]", targetCluster, properties);
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

    private void updateSourceCluster(String clusterName, List<String> properties) {
        validateInput(properties, "properties");

        List<String> policies = listSourceClusterPolicies(targetClient, clusterName);
        performUpdate(clusterName, properties, policies);
    }

    private void updateTargetCluster(String clusterName, List<String> properties) {
        validateInput(properties, "properties");

        List<String> policies = listTargetClusterPolicies(targetClient, clusterName);
        performUpdate(clusterName, properties, policies);
    }

    private void performUpdate(String clusterName, List<String> properties, List<String> policies) {
        // The cluster entity exists on the clusters.
        String targetClusterDefinition = getClusterDefinition(targetClient, clusterName);
        LOG.debug("Before update targetClusterDefinition: {0}", targetClusterDefinition);
        String sourceClusterDefinition = getClusterDefinition(sourceClient, clusterName);
        LOG.debug("Before update sourceClusterDefinition: {0}", sourceClusterDefinition);

        abortCalled=true;
        abortPolicies(targetClient, policies, true);

        suspendCalled = true;
        suspendPolicies(targetClient, policies, true);

        waitBeforeNextOperation();
        LOG.debug("Updating cluster definition on target: {0}", clusterName);
        updateCluster(targetClient, clusterName, properties);
        LOG.debug("Updating cluster definition on source: {0}", clusterName);
        updateCluster(sourceClient, clusterName, properties);

        waitBeforeNextOperation();
        resumePolicies(targetClient, policies, true);
        // reset as all the policies are resumed.
        suspendCalled = false;

        waitBeforeNextOperation();
        rerunPolicyInstance(targetClient, policies, true);
        abortCalled = false;
    }

    private void updateSourceTargetCluster() {
        List<String> sourceClusterProperties = getCombinedProperties(definition.getSourceClusterEndPoints(),
                definition.getSourceClusterProperties());
        LOG.debug("Source cluster update properties: [{0}]", sourceClusterProperties);

        List<String> targetClusterProperties = getCombinedProperties(definition.getTargetClusterEndPoints(),
                definition.getTargetClusterProperties());
        LOG.debug("Target cluster update properties: [{0}]", targetClusterProperties);

        validateInput(sourceClusterProperties, "(sourceClusterEndPoints, sourceClusterProperties)");
        validateInput(targetClusterProperties, "(targetClusterEndPoints, targetClusterProperties(");

        String sourceClusterName = definition.getSourceClusterName();
        String targetClusterName = definition.getTargetClusterName();

        // The cluster entity exists on the clusters. Store the entity into a file.
        String sourceClusterDefinition = getClusterDefinition(targetClient, sourceClusterName);
        LOG.debug("Before update sourceClusterDefinition: {0}", sourceClusterDefinition);
        String targetClusterDefinition = getClusterDefinition(targetClient, targetClusterName);
        LOG.debug("Before update targetClusterDefinition: {0}", targetClusterDefinition);

        String filterBy = "sourceCluster:" + sourceClusterName + ",targetCluster:" + targetClusterName;
        List<String> targetPolicies = listPolicies(targetClient, filterBy);
        filterBy = "sourceCluster:" + targetClusterName + ",targetCluster:" + sourceClusterName;
        List<String> sourcePolicies = listPolicies(targetClient, filterBy);

        abortCalled = true;
        abortPolicies(targetClient, targetPolicies, true);
        abortPolicies(sourceClient, sourcePolicies, false);

        suspendCalled = true;
        suspendPolicies(targetClient, targetPolicies, true);
        suspendPolicies(sourceClient, sourcePolicies, false);

        waitBeforeNextOperation();
        unpairCluster(targetClient, sourceClusterName);

        waitBeforeNextOperation();
        // update on target cluster.
        updateCluster(targetClient, sourceClusterName, sourceClusterProperties);
        updateCluster(targetClient, targetClusterName, targetClusterProperties);

        // update on source cluster.
        updateCluster(sourceClient, sourceClusterName, sourceClusterProperties);
        updateCluster(sourceClient, targetClusterName, targetClusterProperties);

        waitBeforeNextOperation();
        pairCluster(targetClient, sourceClusterName);

        waitBeforeNextOperation();
        resumePolicies(targetClient, targetPolicies, true);
        resumePolicies(sourceClient, sourcePolicies, false);
        // reset as all the policies are resumed.
        suspendCalled = false;

        waitBeforeNextOperation();
        rerunPolicyInstance(targetClient, targetPolicies, true);
        rerunPolicyInstance(sourceClient, sourcePolicies, false);
        abortCalled=false;
    }

    private void validateBeaconEndPoint(String beaconEndPoint) {
        BeaconWebClient client = new BeaconWebClient(beaconEndPoint);
        client.getVersion();
    }

    private void resumePolicies(BeaconWebClient client, List<String> policies, Boolean isTarget) {
        if (policies == null || policies.isEmpty()) {
            LOG.debug("There are no policies to resume. Returning.");
            return;
        }
        for (String policy : policies) {
            LOG.debug("resuming policy: [{0}], beaconServer: [{1}]", policy, client);
            client.resumePolicy(policy);
            if (isTarget != null) {
                if (isTarget) {
                    targetSuspendedPolicies.remove(policy);
                } else {
                    sourceSuspendedPolicies.remove(policy);
                }
            }
        }
    }

    private void suspendPolicies(BeaconWebClient client, List<String> policies, Boolean isTarget) {
        if (policies == null || policies.isEmpty()) {
            LOG.debug("There are no policies to suspend. Returning.");
            return;
        }
        for (String policy : policies) {
            LOG.debug("suspending policy: [{0}], beaconServer: [{1}]", policy, client);
            try {
                client.suspendPolicy(policy);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
            if (isTarget) {
                targetSuspendedPolicies.add(policy);
            } else {
                sourceSuspendedPolicies.add(policy);
            }
        }
    }

    private void abortPolicies(BeaconWebClient client, List<String> policies, Boolean isTarget) {
        if (policies == null || policies.isEmpty()) {
            LOG.debug("There are no policies to abort. Returning.");
            return;
        }
        for (String policy : policies) {
            LOG.debug("aborting policy: [{0}], beaconServer: [{1}]", policy, client);
            client.abortPolicyInstance(policy);
            if (isTarget) {
                targetAbortedPolicies.add(policy);
            } else {
                sourceAbortedPolicies.add(policy);
            }
        }
    }

    private void rerunPolicyInstance(BeaconWebClient client, List<String> policies, Boolean isTarget) {
        if (policies == null || policies.isEmpty()) {
            LOG.debug("There are no policy instances to rerun. Returning.");
            return;
        }
        for (String policy : policies) {
            LOG.debug("rerunning instance of policy: [{0}], beaconServer: [{1}]", policy, client);
            try {
                client.rerunPolicyInstance(policy);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
            if (isTarget != null) {
                if (isTarget) {
                    targetAbortedPolicies.remove(policy);
                } else {
                    sourceAbortedPolicies.remove(policy);
                }
            }
        }
    }

    private String getClusterDefinition(BeaconWebClient client, String clusterName) {
        return client.getCluster(clusterName);
    }

    private void updateCluster(BeaconWebClient client, String clusterName, List<String> properties) {
        String updateDefinition = StringUtils.join(properties, System.lineSeparator());
        LOG.debug("Updating cluster [{0}], beaconServer: [{1}], properties: [{2}]",
                clusterName, client, properties);
        client.updateCluster(clusterName, updateDefinition);
    }

    private void unpairCluster(BeaconWebClient client, String sourceClusterName) {
        LOG.debug("unpair the cluster: [{0}], beaconServer: [{1}]", sourceClusterName, client);
        client.unpairClusters(sourceClusterName, false);
        isUnpairDone = true;
    }

    private void pairCluster(BeaconWebClient client, String sourceClusterName) {
        LOG.debug("pair the cluster: [{0}], beaconServer: [{1}]", sourceClusterName, client);
        client.pairClusters(sourceClusterName, false);
        isUnpairDone = false;
    }

    private List<String> listSourceClusterPolicies(BeaconWebClient client, String clusterName) {
        String filterBy = "sourceCluster:" + clusterName;
        return listPolicies(client, filterBy);
    }

    private List<String> listTargetClusterPolicies(BeaconWebClient client, String clusterName) {
        String filterBy = "targetCluster:" + clusterName;
        return listPolicies(client, filterBy);
    }

    private List<String> listPolicies(BeaconWebClient client, String filterBy) {
        List<String> polices = new ArrayList<>();
        LOG.debug("filter used: [{0}]", filterBy);
        PolicyList policyList = client.getPolicyList("status", null, filterBy, null, null, null);
        PolicyList.PolicyElement[] policyElements = policyList.getElements();
        for (PolicyList.PolicyElement element : policyElements) {
            if (element.status.equalsIgnoreCase(JobStatus.RUNNING.name())) {
                LOG.debug("Policy: [{0}], status: [{1}] found.", element.name, element.status);
                polices.add(element.name);
            }
        }
        LOG.debug("List of polices found: {0} beaconServer: [{1}]", polices, client);
        return polices;
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

    private void waitBeforeNextOperation() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
            // Ignore.
        }
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            if (clusterUpdate != null) {
                LOG.debug("=========Executing ShutdownHook=========");
                // Unpair is successful but pairing is not done.
                if (isUnpairDone) {
                    clusterUpdate.pairCluster(targetClient, definition.getSourceClusterName());
                }

                // If policies are suspended then resume to recover from utility failure.
                if (suspendCalled) {
                    clusterUpdate.resumePolicies(targetClient, targetSuspendedPolicies, null);
                    clusterUpdate.resumePolicies(sourceClient, sourceSuspendedPolicies, null);
                }

                // If policies are aborted then rerun to recover from utility failure.
                if (abortCalled) {
                    clusterUpdate.rerunPolicyInstance(targetClient, targetAbortedPolicies, null);
                    clusterUpdate.rerunPolicyInstance(sourceClient, sourceAbortedPolicies, null);
                }
            }
        }
    }
}
