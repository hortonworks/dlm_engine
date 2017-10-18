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

import java.util.List;

/**
 * Cluster update definition.
 */
public final class ClusterUpdateDefinition {

    private ClusterUpdateDefinition() {
    }

    private String sourceBeaconEndPoint;
    private String targetBeaconEndPoint;
    private String sourceClusterName;
    private String targetClusterName;
    private List<String> sourceClusterEndPoints;
    private List<String> sourceClusterProperties;
    private List<String> targetClusterEndPoints;
    private List<String> targetClusterProperties;

    public String getSourceBeaconEndPoint() {
        return sourceBeaconEndPoint;
    }

    public void setSourceBeaconEndPoint(String sourceBeaconEndPoint) {
        this.sourceBeaconEndPoint = sourceBeaconEndPoint;
    }

    public String getTargetBeaconEndPoint() {
        return targetBeaconEndPoint;
    }

    public void setTargetBeaconEndPoint(String targetBeaconEndPoint) {
        this.targetBeaconEndPoint = targetBeaconEndPoint;
    }

    public String getSourceClusterName() {
        return sourceClusterName;
    }

    public void setSourceClusterName(String sourceClusterName) {
        this.sourceClusterName = sourceClusterName;
    }

    public String getTargetClusterName() {
        return targetClusterName;
    }

    public void setTargetClusterName(String targetClusterName) {
        this.targetClusterName = targetClusterName;
    }

    public List<String> getSourceClusterProperties() {
        return sourceClusterProperties;
    }

    public void setSourceClusterProperties(List<String> sourceClusterProperties) {
        this.sourceClusterProperties = sourceClusterProperties;
    }

    public List<String> getTargetClusterProperties() {
        return targetClusterProperties;
    }

    public void setTargetClusterProperties(List<String> targetClusterProperties) {
        this.targetClusterProperties = targetClusterProperties;
    }

    public List<String> getSourceClusterEndPoints() {
        return sourceClusterEndPoints;
    }

    public void setSourceClusterEndPoints(List<String> sourceClusterEndPoints) {
        this.sourceClusterEndPoints = sourceClusterEndPoints;
    }

    public List<String> getTargetClusterEndPoints() {
        return targetClusterEndPoints;
    }

    public void setTargetClusterEndPoints(List<String> targetClusterEndPoints) {
        this.targetClusterEndPoints = targetClusterEndPoints;
    }
}
