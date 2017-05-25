/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.BeaconStoreService;
import com.hortonworks.beacon.store.bean.EventBean;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Events Bean Executor .
 */
public class EventsExecutor {
    private static final BeaconLog LOG = BeaconLog.getLog(EventsExecutor.class);
    private static final String EVENT_BASE_QUERY = "SELECT OBJECT(a) FROM EventBean a";

    /**
     * Enums for Events named queries.
     */
    public enum EventsQuery {
        GET_EVENTS_FOR_POLICY,
        GET_EVENTS_FOR_INSTANCE_ID,
        GET_EVENTS_FOR_ENTITY_TYPE,
        GET_ALL_EVENTS,
        GET_EVENT_FOR_ID,
        GET_POLICY_ID
    }

    private EntityManager entityManager;

    public EventsExecutor() {
        this.entityManager = ((BeaconStoreService) Services.get()
                .getService(BeaconStoreService.SERVICE_NAME)).getEntityManager();
    }

    public EventBean addEvents(EventBean eventBean) throws BeaconStoreException {
        entityManager.getTransaction().begin();
        entityManager.persist(eventBean);
        entityManager.getTransaction().commit();
        entityManager.close();

        return eventBean;
    }

    public void persistEvents(EventBean eventBean) {
        try {
            addEvents(eventBean);
        } catch (BeaconStoreException e) {
            LOG.error("Exception occurred while adding events : {}", e.getMessage());
        }
    }

    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public Query getEventsQuery(EventsQuery namedQuery, Object... parameters) {
        entityManager = ((BeaconStoreService) Services.get()
                .getService(BeaconStoreService.SERVICE_NAME)).getEntityManager();
        LOG.info("named query : {}", namedQuery.name());
        Query query = entityManager.createNamedQuery(namedQuery.name());

        switch (namedQuery) {
            case GET_EVENTS_FOR_POLICY:
                query.setParameter("policyName", parameters[0]);
                query.setParameter("startTime", new Timestamp(((Date)parameters[1]).getTime()));
                query.setParameter("endTime", new Timestamp(((Date)parameters[2]).getTime()));
                break;
            case GET_EVENTS_FOR_INSTANCE_ID:
                query.setParameter("instanceId", parameters[0]);
                break;
            case GET_POLICY_ID:
                query.setParameter("policyName", parameters[0]);
                break;
            case GET_EVENTS_FOR_ENTITY_TYPE:
                query.setParameter("eventEntityType", parameters[0]);
                query.setParameter("startTime", new Timestamp(((Date)parameters[1]).getTime()));
                query.setParameter("endTime", new Timestamp(((Date)parameters[2]).getTime()));
                break;
            case GET_ALL_EVENTS:
                query.setParameter("startTime", new Timestamp(((Date)parameters[0]).getTime()));
                query.setParameter("endTime", new Timestamp(((Date)parameters[1]).getTime()));
                break;
            case GET_EVENT_FOR_ID:
                query.setParameter("eventId", (Integer)parameters[0]);
                break;
            default:
                throw new IllegalArgumentException("Invalid named query parameter passed: " + namedQuery.name());
        }

        return query;
    }

    public List<EventBean> getEventsWithPolicyName(String policyName, Date startDate, Date endDate,
                                                   int offset, int resultsPage) {
        Query query = getEventsQuery(EventsQuery.GET_EVENTS_FOR_POLICY, policyName, startDate, endDate);
        query.setFirstResult(offset);
        query.setMaxResults(resultsPage);

        LOG.info("Executing query: [{}]", query.toString());
        List resultList = query.getResultList();
        List<EventBean> eventBeanList = new ArrayList<>();
        for (Object result : resultList) {
            eventBeanList.add((EventBean) result);
        }

        return eventBeanList;
    }

    public List<EventBean> getEventsWithNameAndType(int eventId, String eventEntityType,
                                                    Date startDate, Date endDate, int offset, int resultsPage) {
        StringBuilder queryBuilder = new StringBuilder(EVENT_BASE_QUERY);
        Timestamp startTime = new Timestamp(startDate.getTime());
        Timestamp endTime = new Timestamp(endDate.getTime());
        queryBuilder.append(" WHERE a.eventId=").append(eventId);
        if (StringUtils.isNotBlank(eventEntityType)) {
            queryBuilder.append(" AND (a.eventEntityType='").append(eventEntityType).append("')");
        }
        queryBuilder.append(" AND (a.eventTimeStamp BETWEEN '").append(startTime)
                .append("' AND '").append(endTime).append("')");
        queryBuilder.append(" ORDER BY a.eventTimeStamp DESC");
        System.out.println("Formed query :"+queryBuilder.toString());
        Query query = ((BeaconStoreService) Services.get().getService(BeaconStoreService.SERVICE_NAME))
                .getEntityManager().createQuery(queryBuilder.toString());
        query.setFirstResult(offset);
        query.setMaxResults(resultsPage);
        LOG.info("Executing query: [{}]", query.toString());
        List resultList = query.getResultList();
        List<EventBean> eventBeanList = new ArrayList<>();
        for (Object result : resultList) {
            eventBeanList.add((EventBean) result);
        }

        return eventBeanList;
    }

    public List<EventBean> getEntityTypeEvents(String eventEntityType, Date startDate, Date endDate,
                                               int offset, int resultsPage) {
        Query query = getEventsQuery(EventsQuery.GET_EVENTS_FOR_ENTITY_TYPE, eventEntityType, startDate, endDate);
        query.setFirstResult(offset);
        query.setMaxResults(resultsPage);

        LOG.info("Executing query: [{}]", query.toString());
        List resultList = query.getResultList();
        List<EventBean> eventBeanList = new ArrayList<>();
        for (Object result : resultList) {
            eventBeanList.add((EventBean) result);
        }

        return eventBeanList;
    }


    public List<EventBean> getInstanceEvents(String instanceId) {
        Query query = getEventsQuery(EventsQuery.GET_EVENTS_FOR_INSTANCE_ID, instanceId);
        LOG.info("Executing query: [{}]", query.toString());
        List resultList = query.getResultList();
        List<EventBean> eventBeanList = new ArrayList<>();
        for (Object result : resultList) {
            eventBeanList.add((EventBean) result);
        }

        return eventBeanList;
    }

    public List<EventBean> getEventsWithPolicyActionId(String  policyName, int actionId) {
        String instanceId = getPolicyId(policyName)+"@"+actionId;
        Query query = getEventsQuery(EventsQuery.GET_EVENTS_FOR_INSTANCE_ID, instanceId);
        LOG.info("Executing query: [{}]", query.toString());
        List resultList = query.getResultList();
        List<EventBean> eventBeanList = new ArrayList<>();
        for (Object result : resultList) {
            eventBeanList.add((EventBean) result);
        }

        return eventBeanList;
    }

    private String getPolicyId(String policyName) {
        Query query = getEventsQuery(EventsQuery.GET_POLICY_ID, policyName);
        List<String> resultList = query.getResultList();

        return resultList.get(0);
    }


    public List<EventBean> getAllEventsInfo(Date startDate, Date endDate,
                                            int offset, int resultsPage) {
        Query query = getEventsQuery(EventsQuery.GET_ALL_EVENTS, startDate, endDate);
        query.setFirstResult(offset);
        query.setMaxResults(resultsPage);
        List resultList = query.getResultList();
        List<EventBean> eventBeanList = new ArrayList<>();
        for (Object result : resultList) {
            eventBeanList.add((EventBean) result);
        }

        return eventBeanList;
    }
}
