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

import java.util.List;
import java.util.Map;

import com.hortonworks.beacon.authorize.BeaconActionTypes;
import com.hortonworks.beacon.authorize.BeaconResourceTypes;

/**
 * This class contains File Policy definitions.
 */
public class PolicyDef {

    private String policyName;
    private Map<String, List<BeaconActionTypes>> users;
    private Map<String, List<BeaconActionTypes>> groups;
    private Map<BeaconResourceTypes, List<String>> resources;

    public String getPolicyName() {
        return policyName;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }

    public Map<String, List<BeaconActionTypes>> getUsers() {
        return users;
    }

    public void setUsers(Map<String, List<BeaconActionTypes>> users) {
        this.users = users;
    }

    public Map<String, List<BeaconActionTypes>> getGroups() {
        return groups;
    }

    public void setGroups(Map<String, List<BeaconActionTypes>> groups) {
        this.groups = groups;
    }

    public Map<BeaconResourceTypes, List<String>> getResources() {
        return resources;
    }

    public void setResources(Map<BeaconResourceTypes, List<String>> resources) {
        this.resources = resources;
    }

    @Override
    public String toString() {
        return "PolicyDef [policyName=" + policyName + ", users=" + users + ", groups=" + groups + ", resources="
            + resources + "]";
    }

}
