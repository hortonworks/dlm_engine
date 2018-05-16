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

package com.hortonworks.beacon;

import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.service.BeaconStoreService;
import com.hortonworks.beacon.service.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Defines the context to be used across the API call within a single thread.
 */
public final class RequestContext {

    private String requestId;
    private String user;

    private EntityManager entityManager;
    private boolean transaction = false;
    private BeaconLogUtils.Info logPrefix = new BeaconLogUtils.Info();
    private static ThreadLocal<RequestContext> context = new ThreadLocal<>();
    private Map<String, Timer> methodTimers = new HashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(RequestContext.class);

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
            BeaconStoreService service = Services.get().getService(BeaconStoreService.class);
            service.closeEntityManager(entityManager);
        }
        entityManager = null;
        context.remove();
    }

    public String getRequestId() {
        return requestId;
    }

    public Map<String, Timer> getMethodTimers() {
        return methodTimers;
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
            BeaconStoreService service = Services.get().getService(BeaconStoreService.class);
            entityManager = service.getEntityManager();
        }
        return entityManager;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public BeaconLogUtils.Info getLogPrefix() {
        return logPrefix;
    }

    public static void setInitialValue() {
        if (context.get() != null) {
            context.get().clear();
        }
        context.set(new RequestContext());
    }

    public Timer startTimer(String methodName) {
        Timer timer = new Timer();
        methodTimers.put(methodName, timer);
        return timer;
    }

    public void stopTimer(String methodName) {
        if (!methodTimers.containsKey(methodName)) {
            LOG.error("Timer for method {} not started!", methodName);
        } else {
            methodTimers.get(methodName).stop();
        }
    }

    public Timer getTimer(String methodName) {
        if (methodTimers.containsKey(methodName)) {
            return methodTimers.get(methodName);
        }
        LOG.error("Timer for method {} doesn't exist!", methodName);
        return null;
    }
}
