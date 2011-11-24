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
        Query query = getEntityManager().createNamedQuery(PolicyPropertiesQuery.GET_POLICY_PROP.name());
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
        Query nativeQuery = getEntityManager().createQuery(query);
        nativeQuery.setParameter("retirementTime", new Timestamp(retirementTime.getTime()));
        int executeUpdate = nativeQuery.executeUpdate();
        LOG.debug("Records deleted for PolicyPropertiesBean, count [{}]", executeUpdate);
    }
}
