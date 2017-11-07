/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon;

import com.hortonworks.beacon.service.BeaconStoreService;
import com.hortonworks.beacon.service.Services;

import javax.persistence.EntityManager;
import java.util.UUID;

/**
 * Defines the context to be used across the API call within a single thread.
 */
public final class RequestContext {

    private String requestId;
    private EntityManager entityManager;
    private boolean transaction = false;
    private static ThreadLocal<RequestContext> context = new ThreadLocal<RequestContext>() {
        protected RequestContext initialValue() {
            return new RequestContext();
        }
    };

    private RequestContext() {
        this.requestId = UUID.randomUUID().toString();
        this.entityManager = null;
    }

    public static RequestContext get() {
        return context.get();
    }

    public void clear() {
        if (entityManager != null) {
            rollbackTransaction();
            BeaconStoreService service = Services.get().getService(BeaconStoreService.SERVICE_NAME);
            service.closeEntityManager(entityManager);
        }
        entityManager = null;
        context.remove();
    }

    public String getRequestId() {
        return requestId;
    }

    public void startTransaction() {
        entityManager = getEntityManager();
        entityManager.getTransaction().begin();
        transaction = true;
    }

    public void commitTransaction() {
        if (transaction && entityManager.getTransaction().isActive()) {
            entityManager.getTransaction().commit();
            transaction = false;
        }
    }

    public void rollbackTransaction() {
        if (transaction && entityManager.getTransaction().isActive()) {
            entityManager.getTransaction().rollback();
            transaction = false;
        }
    }

    public EntityManager getEntityManager() {
        if (entityManager == null) {
            BeaconStoreService service = Services.get().getService(BeaconStoreService.SERVICE_NAME);
            entityManager = service.getEntityManager();
        }
        return entityManager;
    }
}
