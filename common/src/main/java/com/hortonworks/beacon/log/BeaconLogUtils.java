/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.log;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.NDC;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for BLogs.
 */
public final class BeaconLogUtils {
    private BeaconLogUtils() {
    }

    static void prefixPolicy(String policyName, String policyId, String instanceId) {
        Info info = new Info();
        if (StringUtils.isNotBlank(policyName)) {
            info.setParameter(BeaconLogParams.POLICYNAME.name(), policyName);
        }
        if (StringUtils.isNotBlank(policyId)) {
            info.setParameter(BeaconLogParams.POLICYID.name(), policyId);
        }
        if (StringUtils.isNotBlank(instanceId)) {
            info.setParameter(BeaconLogParams.INSTANCEID.name(), instanceId);
        }
        NDC.push(info.resetPrefix());
    }

    public static void prefixPolicy(String policyName, String policyId) {
        prefixPolicy(policyName, policyId, null);
    }

    public static void prefixPolicy(String policyName) {
        prefixPolicy(policyName, null);
    }

    public static void prefixId(String id) {
        Info info = new Info();
        if (id.contains("@")) {
            String jobId = id.substring(0, id.indexOf("@"));
            info.setParameter(BeaconLogParams.POLICYID.name(), jobId);
            info.setParameter(BeaconLogParams.INSTANCEID.name(), id);
        } else {
            info.setParameter(BeaconLogParams.POLICYID.name(), id);
            info.setParameter(BeaconLogParams.INSTANCEID.name(), "");
        }
        NDC.push(info.resetPrefix());
    }

    public static void deletePrefix(){
        NDC.pop();
    }

    /**
     * Info class to store contextual information to create log prefixes.
     */
    public static class Info {
        private String infoPrefix = "";

        static void reset() {
            BeaconLogParams.clearParams();
        }

        private Map<String, String> parameters = new HashMap<>();

        Info() {
        }

        public void clear() {
            parameters.clear();
            resetPrefix();
        }

        public void setParameter(String name, String value) {
            if (!verifyParameterNames(name)) {
                throw new IllegalArgumentException("Parameter: " + name + " is not defined");
            }
            parameters.put(name, value);
        }

        private boolean verifyParameterNames(String name) {
            return BeaconLogParams.checkParams(name);
        }

        String getParameter(String name) {
            return parameters.get(name);
        }

        void setParameters(Info logInfo) {
            parameters.clear();
            parameters.putAll(logInfo.parameters);
        }

        String createPrefix() {
            StringBuilder sb = new StringBuilder();
            for (int i=0; i<BeaconLogParams.size(); i++){
                String paramName = BeaconLogParams.getParam(i);
                if (parameters.containsKey(paramName)
                        && StringUtils.isNotBlank(parameters.get(paramName))) {
                    sb.append(paramName);
                    sb.append("[");
                    sb.append(parameters.get(paramName));
                    sb.append("] ");
                }
            }

            return sb.toString().trim();
        }

        String resetPrefix() {
            infoPrefix = createPrefix();
            return infoPrefix;
        }

        String getInfoPrefix() {
            return infoPrefix;
        }
    }
}
