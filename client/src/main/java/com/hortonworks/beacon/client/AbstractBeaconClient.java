package com.hortonworks.beacon.client;


import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyList;

public abstract class AbstractBeaconClient {
    public abstract APIResult submitCluster(String clusterName, String filePath);

    public abstract APIResult submitReplicationPolicy(String policyName, String filePath);

    public abstract APIResult scheduleReplicationPolicy(String policyName);

    public abstract APIResult submitAndScheduleReplicationPolicy(String policyName, String filePath);

    public abstract ClusterList getClusterList(String fields, String orderBy, String sortOrder,
                                               Integer offset, Integer numResults);

    public abstract PolicyList getPolicyList(String fields, String orderBy, String sortOrder,
                                             Integer offset, Integer numResults);

    public abstract APIResult getClusterStatus(String clusterName);

    public abstract APIResult getPolicyStatus(String policyName);

    public abstract String getCluster(String clusterName);

    public abstract String getPolicy(String policyName);

    public abstract APIResult deleteCluster(String clusterName);

    public abstract APIResult deletePolicy(String policyName);

    public abstract APIResult suspendPolicy(String policyName);

    public abstract APIResult resumePolicy(String policyName);

    public abstract APIResult pairClusters(String localClusterName, String remoteClusterName,
                                           String remoteBeaconEndpoint);

    public abstract APIResult syncCluster(String clusterName, String clusterDefinition);

    public abstract APIResult syncPolicy(String policyName, String policyDefinition);
}
