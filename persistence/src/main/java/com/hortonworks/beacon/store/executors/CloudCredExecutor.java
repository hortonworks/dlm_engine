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
        getEntityManager().persist(bean);
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
        Query query = getEntityManager().createNamedQuery(namedQuery.name());
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
