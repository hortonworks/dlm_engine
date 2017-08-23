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
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.store.BeaconStoreException;
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
public class EventsExecutor extends BaseExecutor {

    private static final BeaconLog LOG = BeaconLog.getLog(EventsExecutor.class);
    private static final String EVENT_BASE_QUERY = "SELECT OBJECT(a) FROM EventBean a";
    private static final String COUNT_EVENT_QUERY = "SELECT COUNT(a.id) FROM EventBean a";
    private static final String POLICY_NAME_FILTER = " WHERE a.policyId IN (SELECT b.id FROM PolicyBean b"
            + " WHERE b.name=:policyName)";
    private static final String ID_FILTER = " WHERE a.eventId=:eventId";
    private static final String ENTITY_TYPE_FILTER = " WHERE a.eventEntityType=:eventEntityType";

    /**
     * Enums for Events named queries.
     */
    public enum EventsQuery {
        GET_EVENTS_FOR_INSTANCE_ID,
        GET_POLICY_ID
    }

    public EventsExecutor() {
    }

    public EventBean addEvents(EventBean eventBean) throws BeaconStoreException {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            entityManager.getTransaction().begin();
            entityManager.persist(eventBean);
            entityManager.getTransaction().commit();
            return eventBean;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public void persistEvents(EventBean eventBean) {
        try {
            addEvents(eventBean);
        } catch (BeaconStoreException e) {
            LOG.error(MessageCode.PERS_000023.name(), e.getMessage());
        }
    }

    public Query getEventsQuery(EventsQuery namedQuery, EntityManager entityManager, Object... parameters) {
        LOG.info(MessageCode.PERS_000024.name(), namedQuery.name());
        Query query = entityManager.createNamedQuery(namedQuery.name());

        switch (namedQuery) {
            case GET_EVENTS_FOR_INSTANCE_ID:
                query.setParameter("instanceId", parameters[0]);
                break;
            case GET_POLICY_ID:
                query.setParameter("policyName", parameters[0]);
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.PERS_000002.name(), namedQuery.name()));
        }

        return query;
    }

    public long getEventsWithPolicyNameCount(String policyName, Date startDate, Date endDate) {
        EntityManager entityManager = null;
        try {
            String eventQuery = getEventsQuery(COUNT_EVENT_QUERY, POLICY_NAME_FILTER, startDate, endDate,
                    " ", " ");
            entityManager = STORE.getEntityManager();
            Query query = entityManager.createQuery(eventQuery);
            query.setParameter("policyName", policyName);
            return (long)query.getResultList().get(0);
        }catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public List<EventBean> getEventsWithPolicyName(String policyName, Date startDate, Date endDate,
                                                   String orderBy, String sortBy,
                                                   int offset, int resultsPage) {
        EntityManager entityManager = null;
        try {
            String eventQuery = getEventsQuery(EVENT_BASE_QUERY, POLICY_NAME_FILTER, startDate, endDate,
                    orderBy, sortBy);
            entityManager = STORE.getEntityManager();
            Query query = entityManager.createQuery(eventQuery);
            query.setParameter("policyName", policyName);
            query.setFirstResult(offset);
            query.setMaxResults(resultsPage);

            LOG.info(MessageCode.PERS_000025.name(), query.toString());
            List resultList = query.getResultList();
            List<EventBean> eventBeanList = new ArrayList<>();
            for (Object result : resultList) {
                eventBeanList.add((EventBean) result);
            }
            return eventBeanList;
        }catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public long getEventsWithNameCount(int eventId, Date startDate, Date endDate) {
        EntityManager entityManager = null;
        try {
            String eventQuery = getEventsQuery(COUNT_EVENT_QUERY, ID_FILTER, startDate, endDate,
                    " ", " ");
            entityManager = STORE.getEntityManager();
            Query query = entityManager.createQuery(eventQuery);
            query.setParameter("eventId", eventId);
            return (long)query.getResultList().get(0);
        }catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public List<EventBean> getEventsWithName(int eventId, Date startDate, Date endDate,
                                             String orderBy, String sortBy,
                                             int offset, int resultsPage) {
        EntityManager entityManager = null;
        try {
            String eventQuery = getEventsQuery(EVENT_BASE_QUERY, ID_FILTER, startDate, endDate,
                    orderBy, sortBy);
            entityManager = STORE.getEntityManager();
            Query query = entityManager.createQuery(eventQuery);
            query.setParameter("eventId", eventId);
            query.setFirstResult(offset);
            query.setMaxResults(resultsPage);
            LOG.info(MessageCode.PERS_000025.name(), query.toString());
            List resultList = query.getResultList();
            List<EventBean> eventBeanList = new ArrayList<>();
            for (Object result : resultList) {
                eventBeanList.add((EventBean) result);
            }
            return eventBeanList;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public long getEntityTypeEventsCount(String eventEntityType, Date startDate, Date endDate) {
        EntityManager entityManager = null;
        try {
            String eventQuery = getEventsQuery(COUNT_EVENT_QUERY, ENTITY_TYPE_FILTER, startDate, endDate,
                    " ", " ");
            entityManager = STORE.getEntityManager();
            Query query = entityManager.createQuery(eventQuery);
            query.setParameter("eventEntityType", eventEntityType);
            return (long)query.getResultList().get(0);
        }catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public List<EventBean> getEntityTypeEvents(String eventEntityType, Date startDate, Date endDate,
                                               String orderBy, String sortBy,
                                               int offset, int resultsPage) {
        EntityManager entityManager = null;
        try {
            String eventQuery = getEventsQuery(EVENT_BASE_QUERY, ENTITY_TYPE_FILTER, startDate, endDate,
                    orderBy, sortBy);
            entityManager = STORE.getEntityManager();
            Query query = entityManager.createQuery(eventQuery);
            query.setParameter("eventEntityType", eventEntityType);
            query.setFirstResult(offset);
            query.setMaxResults(resultsPage);

            LOG.info(MessageCode.PERS_000025.name(), query.toString());
            List resultList = query.getResultList();
            List<EventBean> eventBeanList = new ArrayList<>();
            for (Object result : resultList) {
                eventBeanList.add((EventBean) result);
            }
            return eventBeanList;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }


    public List<EventBean> getInstanceEvents(String instanceId) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = getEventsQuery(EventsQuery.GET_EVENTS_FOR_INSTANCE_ID, entityManager, instanceId);
            LOG.info(MessageCode.PERS_000025.name(), query.toString());
            List resultList = query.getResultList();
            List<EventBean> eventBeanList = new ArrayList<>();
            for (Object result : resultList) {
                eventBeanList.add((EventBean) result);
            }
            return eventBeanList;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public List<EventBean> getEventsWithPolicyActionId(String  policyName, int actionId) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            String instanceId = getPolicyId(policyName) + "@" + actionId;
            Query query = getEventsQuery(EventsQuery.GET_EVENTS_FOR_INSTANCE_ID, entityManager, instanceId);
            LOG.info(MessageCode.PERS_000025.name(), query.toString());
            List resultList = query.getResultList();
            List<EventBean> eventBeanList = new ArrayList<>();
            for (Object result : resultList) {
                eventBeanList.add((EventBean) result);
            }
            return eventBeanList;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    private String getPolicyId(String policyName) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = getEventsQuery(EventsQuery.GET_POLICY_ID, entityManager, policyName);
            List<String> resultList = query.getResultList();
            return resultList.get(0);
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }


    public List<EventBean> getAllEventsInfo(Date startDate, Date endDate, String orderBy, String sortBy,
                                            int offset, int resultsPage) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            String eventInfoQuery = getEventsQuery(EVENT_BASE_QUERY, " ", startDate, endDate,
                    orderBy, sortBy);
            Query query = entityManager.createQuery(eventInfoQuery);
            query.setFirstResult(offset);
            query.setMaxResults(resultsPage);
            LOG.info(MessageCode.PERS_000026.name(), query.toString());
            List resultList = query.getResultList();
            List<EventBean> eventBeanList = new ArrayList<>();
            for (Object result : resultList) {
                eventBeanList.add((EventBean) result);
            }
            return eventBeanList;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public long getAllEventsInfoCount(Date startDate, Date endDate) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            String eventInfoQuery = getEventsQuery(COUNT_EVENT_QUERY, " ", startDate, endDate,
                    " ", " ");
            Query query = entityManager.createQuery(eventInfoQuery);
            LOG.info(MessageCode.PERS_000026.name(), query.toString());
            return (long)query.getResultList().get(0);
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    private String getEventsQuery(String query, String filter, Date startDate, Date endDate,
                                  String orderBy, String sortBy) {
        StringBuilder queryBuilder = new StringBuilder(query);

        if (StringUtils.isNotBlank(filter)) {
            queryBuilder.append(filter);
        }

        if (startDate!=null || endDate!=null) {
            queryBuilder.append(" WHERE ").append(getTimeStampQuery(startDate, endDate));
        }

        if (StringUtils.isNotBlank(orderBy) && StringUtils.isNotBlank(sortBy)) {
            queryBuilder.append(getOrderQuery(orderBy, sortBy));
        }

        return queryBuilder.toString();
    }

    private String getTimeStampQuery(Date startDate, Date endDate) {
        StringBuilder timeStampBuilder = new StringBuilder();
        Timestamp startTime = (startDate!=null) ? new Timestamp(startDate.getTime()) : null;
        Timestamp endTime = (endDate!=null) ? new Timestamp(endDate.getTime()) : null;

        if (startTime != null && endTime != null) {
            timeStampBuilder.append(" a.eventTimeStamp BETWEEN '").append(startTime)
                    .append("' AND '").append(endTime).append("'");
        } else if (startTime != null) {
            timeStampBuilder.append(" a.eventTimeStamp >= '").append(startTime).append("'");
        } else if (endTime != null) {
            timeStampBuilder.append(" a.eventTimeStamp <= '").append(endTime).append("'");
        }

        return timeStampBuilder.toString();
    }

    private String getOrderQuery(String orderBy, String sortBy) {
        return " ORDER BY a."+orderBy+' '+sortBy;
    }
}
