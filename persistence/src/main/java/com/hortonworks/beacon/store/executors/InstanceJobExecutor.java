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

import com.hortonworks.beacon.store.bean.InstanceJobBean;
import com.hortonworks.beacon.util.StringFormat;

import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Beacon store executor for instance jobs.
 */
public class InstanceJobExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(InstanceJobExecutor.class);
    private InstanceJobBean bean;

    /**
     * Enums for InstanceJob named queries.
     */
    public enum InstanceJobQuery {
        GET_INSTANCE_JOB,
        UPDATE_STATUS_START,
        INSTANCE_JOB_UPDATE_STATUS,
        INSTANCE_JOB_REMAIN_RETIRE,
        UPDATE_JOB_COMPLETE,
        UPDATE_JOB_FAIL_RETIRE,
        UPDATE_JOB_RETRY_COUNT,
        DELETE_INSTANCE_JOB,
        DELETE_RETIRED_JOBS
    }

    public InstanceJobExecutor(InstanceJobBean bean) {
        this.bean = bean;
    }

    public void execute() {
        entityManager.persist(bean);
    }

    public void executeUpdate(InstanceJobQuery namedQuery) {
        Query query = getQuery(namedQuery);
        int update = query.executeUpdate();
        LOG.debug("Records updated for InstanceJobBean table namedQuery [{}], count [{}]", namedQuery, update);
    }

    private Query getQuery(InstanceJobQuery namedQuery) {
        Query query = entityManager.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_STATUS_START:
                query.setParameter("status", bean.getStatus());
                query.setParameter("startTime", bean.getStartTime());
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("offset", bean.getOffset());
                break;
            case INSTANCE_JOB_UPDATE_STATUS:
                query.setParameter("status", bean.getStatus());
                query.setParameter("instanceId", bean.getInstanceId());
                break;
            case INSTANCE_JOB_REMAIN_RETIRE:
                query.setParameter("status", bean.getStatus());
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("retirementTime", bean.getRetirementTime());
                break;
            case UPDATE_JOB_COMPLETE:
                query.setParameter("status", bean.getStatus());
                query.setParameter("message", bean.getMessage());
                query.setParameter("endTime", bean.getEndTime());
                query.setParameter("contextData", bean.getContextData());
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("offset", bean.getOffset());
                break;
            case UPDATE_JOB_FAIL_RETIRE:
                query.setParameter("status", bean.getStatus());
                query.setParameter("message", bean.getMessage());
                query.setParameter("endTime", bean.getEndTime());
                query.setParameter("contextData", bean.getContextData());
                query.setParameter("retirementTime", bean.getRetirementTime());
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("offset", bean.getOffset());
                break;
            case UPDATE_JOB_RETRY_COUNT:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("offset", bean.getOffset());
                query.setParameter("runCount", bean.getRunCount());
                break;
            case GET_INSTANCE_JOB:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("offset", bean.getOffset());
                break;
            case DELETE_INSTANCE_JOB:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("retirementTime", bean.getRetirementTime());
                break;
            case DELETE_RETIRED_JOBS:
                query.setParameter("retirementTime", new Timestamp(bean.getRetirementTime().getTime()));
                break;
            default:
                throw new IllegalArgumentException(
                    StringFormat.format("Invalid named query parameter passed: {}", namedQuery.name()));
        }
        return query;
    }

    public InstanceJobBean getInstanceJob(InstanceJobQuery namedQuery) {
        Query query = getQuery(namedQuery);
        return (InstanceJobBean) query.getSingleResult();
    }
}
