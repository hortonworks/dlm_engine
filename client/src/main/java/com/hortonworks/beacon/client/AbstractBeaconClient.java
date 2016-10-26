package com.hortonworks.beacon.client;


import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.EntityList;

public abstract class AbstractBeaconClient {
    public abstract APIResult submitCluster(String clusterName, String filePath);

    public abstract APIResult submitReplicationPolicy(String policyName, String filePath);

    public abstract APIResult scheduleReplicationPolicy(String policyName);

    public abstract APIResult submitAndScheduleReplicationPolicy(String policyName, String filePath);

    public abstract EntityList getClusterList(String fields, String orderBy, String sortOrder,
                                              Integer offset, Integer numResults);

    public abstract EntityList getPolicyList(String fields, String orderBy, String sortOrder,
                                             Integer offset, Integer numResults);

    public abstract APIResult getClusterStatus(String clusterName);

    public abstract APIResult getPolicyStatus(String policyName);

    public abstract String getCluster(String clusterName);

    public abstract String getPolicy(String policyName);

    public abstract APIResult deleteCluster(String clusterName);

    public abstract APIResult deletePolicy(String policyName);

    public abstract APIResult suspendPolicy(String policyName);

    public abstract APIResult resumePolicy(String policyName);
}
