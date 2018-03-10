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

import com.hortonworks.beacon.store.bean.CloudCredBean;
import com.hortonworks.beacon.util.StringFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Query;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Beacon store executor for cloud cred entity.
 */
public class CloudCredExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(CloudCredExecutor.class);

    private CloudCredBean bean;

    /**
     * Query enums for cloud cred entity.
     */
    public enum CloudCredQuery {
        GET_CLOUD_CRED,
        UPDATE_CLOUD_CRED,
        DELETE_CLOUD_CRED;
    }

    public CloudCredExecutor(CloudCredBean bean) {
        this.bean = bean;
    }

    public void submit() {
        Date currentTime = new Date();
        bean.setCreationTime(currentTime);
        bean.setLastModifiedTime(currentTime);
        entityManager.persist(bean);
    }

    public CloudCredBean get(CloudCredQuery namedQuery) {
        Query query = getQuery(namedQuery);
        List resultList = query.getResultList();
        if (resultList == null || resultList.isEmpty()) {
            throw new NoSuchElementException(
                    StringFormat.format("CloudCred does not exists for id: [{}]", bean.getId()));
        }
        return (CloudCredBean)resultList.get(0);
    }

    private Query getQuery(CloudCredQuery namedQuery) {
        Query query = entityManager.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_CLOUD_CRED:
                query.setParameter("id", bean.getId());
                break;
            case UPDATE_CLOUD_CRED:
                query.setParameter("configuration", bean.getConfiguration());
                query.setParameter("lastModifiedTime", bean.getLastModifiedTime());
                query.setParameter("id", bean.getId());
                query.setParameter("authType", bean.getAuthType());
                break;
            case DELETE_CLOUD_CRED:
                query.setParameter("id", bean.getId());
                break;
            default:
                throw new IllegalArgumentException(
                        StringFormat.format("Invalid named query parameter passed: {}", namedQuery.name()));
        }
        return query;
    }

    public void update(CloudCredQuery namedQuery) {
        Query query = getQuery(namedQuery);
        int executeUpdate = query.executeUpdate();
        LOG.debug("Query [{}] updated [{}] records.", namedQuery, executeUpdate);
    }

    public void delete(CloudCredQuery namedQuery) {
        Query query = getQuery(namedQuery);
        int executeUpdate = query.executeUpdate();
        LOG.debug("Query [{}] updated [{}] records.", namedQuery, executeUpdate);
    }
}
