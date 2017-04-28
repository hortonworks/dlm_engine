/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.beacon.log;

import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for BLogs.
 */
public final class BeaconLogUtils {
    private BeaconLogUtils() {
    }


    public static void setLogInfo(String userId, String clusterName) {
        BeaconLog.Info info = BeaconLog.Info.get();
        info.setParameter(BeaconLogParams.USER.name(), userId);
        info.setParameter(BeaconLogParams.CLUSTER.name(), clusterName);
        info.resetPrefix();
    }

    public static void setLogInfo(String userName, String clusterName, String policyName,
                                  String policyId, String instanceId) {
        BeaconLog.Info info = BeaconLog.Info.get();
        info.setParameter(BeaconLogParams.USER.name(), userName);
        info.setParameter(BeaconLogParams.CLUSTER.name(), clusterName);
        info.setParameter(BeaconLogParams.POLICYNAME.name(), policyName);
        info.setParameter(BeaconLogParams.POLICYID.name(), policyId);
        info.setParameter(BeaconLogParams.INSTANCEID.name(), instanceId);
        info.resetPrefix();
    }

    public static void setLogInfo(String id) {
        BeaconLog.Info info = BeaconLog.Info.get();
        if (id.contains("@")) {
            String jobId = id.substring(0, id.indexOf("@"));
            info.setParameter(BeaconLogParams.POLICYID.name(), jobId);
            info.setParameter(BeaconLogParams.INSTANCEID.name(), id);
        } else {
            info.setParameter(BeaconLogParams.POLICYID.name(), id);
            info.setParameter(BeaconLogParams.INSTANCEID.name(), "");
        }
        info.resetPrefix();
    }

    public static BeaconLog setLogInfo(BeaconLog log, String userName, String clusterName,
                                       String policyId, String instanceId) {
        BeaconLog.Info info = BeaconLog.Info.get();
        clearLogPrefix();
        if (StringUtils.isNotBlank(userName)) {
            info.setParameter(BeaconLogParams.USER.name(), userName);
        }
        if (StringUtils.isNotBlank(clusterName)) {
            info.setParameter(BeaconLogParams.CLUSTER.name(), clusterName);
        }
        if (StringUtils.isNotBlank(policyId)) {
            info.setParameter(BeaconLogParams.POLICYID.name(), policyId);
        }
        if (StringUtils.isNotBlank(instanceId)) {
            info.setParameter(BeaconLogParams.INSTANCEID.name(), instanceId);
        }

        return BeaconLog.resetPrefix(log);
    }

    public static void clearLogPrefix() {
        BeaconLog.Info info = BeaconLog.Info.get();
        info.clearParameter(BeaconLogParams.USER.name());
        info.clearParameter(BeaconLogParams.CLUSTER.name());
        info.clearParameter(BeaconLogParams.POLICYNAME.name());
        info.clearParameter(BeaconLogParams.POLICYID.name());
        info.clearParameter(BeaconLogParams.INSTANCEID.name());
        info.resetPrefix();
    }
}
