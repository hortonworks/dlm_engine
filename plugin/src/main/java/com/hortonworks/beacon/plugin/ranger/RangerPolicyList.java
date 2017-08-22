/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.plugin.ranger;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;


import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/** RangerPolicyList class to contain List of RangerPolicy objects.
*
*/
@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerPolicyList {
    private static final long serialVersionUID = 1L;

    private List<RangerPolicy> policies = new ArrayList<RangerPolicy>();

    public RangerPolicyList() {
        super();
    }

    public RangerPolicyList(List<RangerPolicy> objList) {
        this.policies = objList;
    }

    public List<RangerPolicy> getPolicies() {
        return policies;
    }

    public void setPolicies(List<RangerPolicy> policies) {
        this.policies = policies;
    }

    public int getListSize() {
        if (policies != null) {
            return policies.size();
        }
        return 0;
    }


    public List<?> getList() {
        return policies;
    }

}
