/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.store.bean.PolicyPropertiesBean;

import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Beacon store executor for policy properties.
 */
public class PolicyPropertiesExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyPropertiesBean.class);
    private String policyId;

    public PolicyPropertiesExecutor(String policyId) {
        this.policyId = policyId;
    }

    public PolicyPropertiesExecutor() {
    }

    /**
     * Enums for PolicyProperties named queries.
     */
    public enum PolicyPropertiesQuery {
        GET_POLICY_PROP
    }

    List<PolicyPropertiesBean> getPolicyProperties() {
        Query query = entityManager.createNamedQuery(PolicyPropertiesQuery.GET_POLICY_PROP.name());
        query.setParameter("policyId", policyId);
        List resultList = query.getResultList();
        List<PolicyPropertiesBean> beans = new ArrayList<>();
        if (resultList != null && !resultList.isEmpty()) {
            for (Object result : resultList) {
                beans.add((PolicyPropertiesBean) result);
            }
        }
        return beans;
    }

    public void deleteRetiredPolicyProps(Date retirementTime) {
        String query = "delete from PolicyPropertiesBean pp where "
                + "pp.policyId IN (select b.id from PolicyBean b where b.retirementTime < :retirementTime)";
        Query nativeQuery = entityManager.createQuery(query);
        nativeQuery.setParameter("retirementTime", new Timestamp(retirementTime.getTime()));
        int executeUpdate = nativeQuery.executeUpdate();
        LOG.debug("Records deleted for PolicyPropertiesBean, count [{}]", executeUpdate);
    }
}
