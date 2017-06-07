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

package com.hortonworks.beacon.authorize.simple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hortonworks.beacon.authorize.BeaconActionTypes;
import com.hortonworks.beacon.authorize.BeaconResourceTypes;
import com.hortonworks.beacon.log.BeaconLog;
/**
 * This is utility class for policies.
 */
public final class PolicyUtil {

    private static BeaconLog logger = BeaconLog.getLog(PolicyUtil.class);
    private static boolean isDebugEnabled = logger.isDebugEnabled();

    private PolicyUtil(){
    }
    public static Map<String, Map<BeaconResourceTypes, List<String>>> createPermissionMap(List<PolicyDef> policyDefList,
        BeaconActionTypes permissionType, SimpleBeaconAuthorizer.BeaconAccessorTypes principalType) {
        if (isDebugEnabled) {
            logger.debug("==> PolicyUtil createPermissionMap\nCreating Permission Map for :: {} & {}", permissionType,
                    principalType);
        }
        Map<String, Map<BeaconResourceTypes, List<String>>> userReadMap = new HashMap<>();

        // Iterate over the list of policies to create map
        for (PolicyDef policyDef : policyDefList) {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing policy def : {}", policyDef);
            }

            Map<String, List<BeaconActionTypes>> principalMap =
                principalType.equals(SimpleBeaconAuthorizer.BeaconAccessorTypes.USER) ? policyDef.getUsers() : policyDef
                    .getGroups();
            // For every policy extract the resource list and populate the user map
            for (Entry<String, List<BeaconActionTypes>> e : principalMap.entrySet()) {
                // Check if the user has passed permission type like READ
                if (!e.getValue().contains(permissionType)) {
                    continue;
                }
                // See if the current user is already added to map
                String username = e.getKey();
                Map<BeaconResourceTypes, List<String>> userResourceList = userReadMap.get(username);

                // If its not added then create a new resource list
                if (userResourceList == null) {
                    if (isDebugEnabled) {
                        logger.debug("Resource list not found for {}, creating it", username);
                    }
                    userResourceList = new HashMap<>();
                }
                /*
                 * Iterate over resources from the current policy def and update the resource list for the current user
                 */
                if (policyDef.getResources()!=null) {
                    for (Entry<BeaconResourceTypes, List<String>> resourceTypeMap : policyDef.getResources()
                            .entrySet()) {
                        // For the current resourceType in the policyDef, get the
                        // current list of resources already added
                        BeaconResourceTypes type = resourceTypeMap.getKey();
                        List<String> resourceList = userResourceList.get(type);

                        if (resourceList == null) {
                            // if the resource list was not added for this type then
                            // create and add all the resources in this policy
                            resourceList = new ArrayList<>();
                            resourceList.addAll(resourceTypeMap.getValue());
                        } else {
                            // if the resource list is present then merge both the
                            // list
                            resourceList.removeAll(resourceTypeMap.getValue());
                            resourceList.addAll(resourceTypeMap.getValue());
                        }

                        userResourceList.put(type, resourceList);
                    }
                }
                userReadMap.put(username, userResourceList);

                if (logger.isDebugEnabled()) {
                    logger.debug("userReadMap {}", userReadMap);
                }
            }
        }
        if (isDebugEnabled) {
            logger.debug("Returning Map for {} :: {}", principalType, userReadMap);
            logger.debug("<== PolicyUtil createPermissionMap");
        }
        return userReadMap;

    }
}
