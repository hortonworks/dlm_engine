/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
