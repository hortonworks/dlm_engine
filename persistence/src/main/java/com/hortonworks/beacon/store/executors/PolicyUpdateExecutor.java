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

import com.hortonworks.beacon.api.PropertiesIgnoreCase;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyPropertiesBean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Query;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Beacon store executor for Policy entity update.
 */
public class PolicyUpdateExecutor  extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyUpdateExecutor.class);

    private static final String UPDATE_POLICY = "UPDATE PolicyBean b SET ";

    public PolicyUpdateExecutor() {
    }

    public void persistUpdatedPolicy(PolicyBean updatedPolicy, PropertiesIgnoreCase updatedProps,
                                      PropertiesIgnoreCase newProps) {
        Query policyUpdateQuery = createPolicyUpdateQuery1(updatedPolicy);
        List<Query> policyPropUpdateQuery = createPolicyPropUpdateQuery(updatedPolicy, updatedProps);
        List<PolicyPropertiesBean> policyPropertiesBeans = insertNewPolicyProps(updatedPolicy, newProps);
        int n = policyUpdateQuery.executeUpdate();
        LOG.info("Updated [{}] record", n);
        for (Query query : policyPropUpdateQuery) {
            query.executeUpdate();
        }
        for (PolicyPropertiesBean bean : policyPropertiesBeans) {
            getEntityManager().persist(bean);
        }
    }

    public void persistNewPolicyProperty(PolicyBean updatedPolicy, PropertiesIgnoreCase newProps) {
        List<PolicyPropertiesBean> policyPropertiesBeans = insertNewPolicyProps(updatedPolicy, newProps);
        for (PolicyPropertiesBean bean : policyPropertiesBeans) {
            getEntityManager().persist(bean);
        }
        LOG.info("Updated [{}] record", policyPropertiesBeans.size());
    }

    private List<PolicyPropertiesBean> insertNewPolicyProps(PolicyBean updatedPolicy,
                                                              PropertiesIgnoreCase newProps) {
        List<PolicyPropertiesBean> propBeans = new ArrayList<>();
        Date createdTime = new Date();
        for (String property : newProps.stringPropertyNames()) {
            PolicyPropertiesBean propBean = new PolicyPropertiesBean();
            propBean.setPolicyId(updatedPolicy.getId());
            propBean.setName(property);
            propBean.setValue(newProps.getPropertyIgnoreCase(property));
            propBean.setCreationTime(createdTime);
            propBeans.add(propBean);
            LOG.debug("Policy new custom properties key: [{}], value: [{}]",
                    property, newProps.getPropertyIgnoreCase(property));
        }
        return propBeans;
    }

    private List<Query> createPolicyPropUpdateQuery(PolicyBean updatedPolicy, PropertiesIgnoreCase updatedProps) {
        List<Query> policyPropUpdateQueries = new ArrayList<>();
        for (String property : updatedProps.stringPropertyNames()) {
            Query query = getEntityManager().createNamedQuery(
                    PolicyPropertiesExecutor.PolicyPropertiesQuery.UPDATE_POLICY_PROP.name());
            LOG.debug("Policy custom properties update query: [{}]", query.toString());
            query.setParameter("policyIdParam", updatedPolicy.getId());
            query.setParameter("nameParam", property);
            query.setParameter("valueParam", updatedProps.getPropertyIgnoreCase(property));
            policyPropUpdateQueries.add(query);
        }
        return policyPropUpdateQueries;
    }

    private Query createPolicyUpdateQuery1(PolicyBean updatedPolicy) {
        StringBuilder queryBuilder = new StringBuilder();
        int index = 1;
        List<String> paramNames = new ArrayList<>();
        List<Object> paramValues = new ArrayList<>();
        String key;

        queryBuilder.append("b.changeId = b.changeId+1");

        if (StringUtils.isNotBlank(updatedPolicy.getDescription())) {
            key = "description" + index++;
            queryBuilder.append(", b.description = :").append(key);
            paramNames.add(key);
            paramValues.add(updatedPolicy.getDescription());
        }
        if (updatedPolicy.getStartTime() != null) {
            key = "startTime" + index++;
            queryBuilder.append(", b.startTime = :").append(key);
            paramNames.add(key);
            paramValues.add(updatedPolicy.getStartTime());
        }
        if (updatedPolicy.getEndTime() != null) {
            key = "endTime" + index++;
            queryBuilder.append(", b.endTime = :").append(key);
            paramNames.add(key);
            paramValues.add(updatedPolicy.getEndTime());
        }
        if (updatedPolicy.getPlugins() != null) {
            key = "plugins" + index++;
            queryBuilder.append(", b.plugins = :").append(key);
            paramNames.add(key);
            paramValues.add(updatedPolicy.getPlugins());
        }
        key = "frequencyInSec" + index++;
        queryBuilder.append(", b.frequencyInSec = :").append(key);
        paramNames.add(key);
        paramValues.add(updatedPolicy.getFrequencyInSec());

        key = "id" + index;
        queryBuilder.append(" where b.id = :").append(key);
        paramNames.add(key);
        paramValues.add(updatedPolicy.getId());

        queryBuilder.append(" AND b.retirementTime IS NULL");

        String query = UPDATE_POLICY + queryBuilder.toString();
        LOG.debug("Policy update query: [{}]", query);
        Query updateQuery = getEntityManager().createQuery(query);
        for (int i = 0; i < paramNames.size(); i++) {
            updateQuery.setParameter(paramNames.get(i), paramValues.get(i));
        }
        return updateQuery;
    }
}

