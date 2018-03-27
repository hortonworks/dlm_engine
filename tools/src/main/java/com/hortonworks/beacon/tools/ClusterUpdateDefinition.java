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
