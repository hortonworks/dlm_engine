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

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.store.bean.PolicyPropertiesBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Beacon store executor for policy properties.
 */
public class PolicyPropertiesExecutor extends BaseExecutor {

    private static final BeaconLog LOG = BeaconLog.getLog(PolicyPropertiesBean.class);
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
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
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
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public void deleteRetiredPolicyProps(Date retirementTime) {
        EntityManager entityManager = null;
        try {
            String query = "delete from PolicyPropertiesBean pp where "
                    + "pp.policyId IN (select b.id from PolicyBean b where b.retirementTime < :retirementTime)";
            entityManager = STORE.getEntityManager();
            Query nativeQuery = entityManager.createQuery(query);
            entityManager.getTransaction().begin();
            nativeQuery.setParameter("retirementTime", new Timestamp(retirementTime.getTime()));
            int executeUpdate = nativeQuery.executeUpdate();
            LOG.debug("Records deleted for PolicyPropertiesBean, count [{0}]", executeUpdate);
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }
}
