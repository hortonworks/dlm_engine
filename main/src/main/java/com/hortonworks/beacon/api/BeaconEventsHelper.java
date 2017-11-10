/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.api.result.EventsResult;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.entity.util.PolicyPersistenceHelper;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.EventInfo;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.EventBean;
import com.hortonworks.beacon.store.executors.EventsExecutor;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Helper class for Handling Beacon Events.
 */
public final class BeaconEventsHelper {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconEventsHelper.class);
    private static final long SECOND_IN_MILLIS = 1000L;
    private static final int DEFAULT_FREQUENCY_IN_SECOND = 300;

    private BeaconEventsHelper(){
    }

    static EventsResult getEventsWithPolicyName(String policyName, String startDate, String endDate,
                                                String orderBy, String sortBy, int offset, int resultsPage) {
        EventsExecutor eventExecutor = new EventsExecutor();
        int frequency = getPolicyFrequency(policyName);
        Date endDateTime = StringUtils.isBlank(endDate) ? null : getEndDate(endDate);
        Date startDateTime = StringUtils.isBlank(startDate)
                ? null : getStartDate(startDate, endDateTime, frequency, resultsPage);
        long totalResults = eventExecutor.getEventsWithPolicyNameCount(policyName, startDateTime, endDateTime);
        List<EventBean> beanList = eventExecutor.getEventsWithPolicyName(policyName,
                startDateTime, endDateTime, orderBy, sortBy, offset, resultsPage);

        return getEventsResult(beanList, totalResults);
    }

    static EventsResult getEventsWithName(int eventId, String startDate, String endDate,
                                          String orderBy, String sortBy, Integer offset, Integer resultsPage) {
        EventsExecutor eventExecutor = new EventsExecutor();
        Date endDateTime = StringUtils.isBlank(endDate) ? null : getEndDate(endDate);
        Date startDateTime = StringUtils.isBlank(startDate)
                ? null : getStartDate(startDate, endDateTime, DEFAULT_FREQUENCY_IN_SECOND, resultsPage);
        long totalResults = eventExecutor.getEventsWithNameCount(eventId, startDateTime, endDateTime);
        List<EventBean> beanList = eventExecutor.getEventsWithName(eventId, startDateTime, endDateTime,
                orderBy, sortBy, offset, resultsPage);

        return getEventsResult(beanList, totalResults);
    }

    static EventsResult getEntityTypeEvents(String eventEntityType, String startDate, String endDate,
                                            String orderBy, String sortBy,
                                            Integer offset, Integer resultsPage) {
        LOG.info("Get events for type: {}", eventEntityType);
        EventsExecutor eventExecutor = new EventsExecutor();
        Date endDateTime = StringUtils.isBlank(endDate) ? null : getEndDate(endDate);
        Date startDateTime = StringUtils.isBlank(startDate)
                ? null : getStartDate(startDate, endDateTime, DEFAULT_FREQUENCY_IN_SECOND, resultsPage);
        long totalResults = eventExecutor.getEntityTypeEventsCount(eventEntityType, startDateTime, endDateTime);
        List<EventBean> beanList = eventExecutor.getEntityTypeEvents(eventEntityType,
                startDateTime, endDateTime, orderBy, sortBy, offset, resultsPage);

        return getEventsResult(beanList, totalResults);
    }

    static EventsResult getInstanceEvents(String instanceId) {
        EventsExecutor eventExecutor = new EventsExecutor();
        List<EventBean> beanList = eventExecutor.getInstanceEvents(instanceId);

        return getEventsResult(beanList);
    }

    static EventsResult getEventsWithPolicyActionId(String policyName, int actionid) {
        EventsExecutor eventExecutor = new EventsExecutor();
        List<EventBean> beanList = eventExecutor.getEventsWithPolicyActionId(policyName, actionid);

        return getEventsResult(beanList);
    }

    static EventsResult getAllEventsInfo(String startDate, String endDate, String orderBy, String sortBy,
                                         Integer offset, Integer resultsPage) {
        EventsExecutor eventExecutor = new EventsExecutor();
        Date endDateTime = StringUtils.isBlank(endDate) ? null : getEndDate(endDate);
        Date startDateTime = StringUtils.isBlank(startDate)
                ? null : getStartDate(startDate, endDateTime, DEFAULT_FREQUENCY_IN_SECOND, resultsPage);
        long totalResults = eventExecutor.getAllEventsInfoCount(startDateTime, endDateTime);
        List<EventBean> beanList = eventExecutor.getAllEventsInfo(startDateTime, endDateTime, orderBy, sortBy,
                offset, resultsPage);

        return getEventsResult(beanList, totalResults);
    }

    static EventsResult getSupportedEventDetails() {
        List<String> eventNameList = new ArrayList<>();
        for (Events events : Events.values()) {
            eventNameList.add(events.getName());
        }

        return getEventsList(eventNameList);
    }

    private static EventsResult getEventsResult(List<EventBean> eventBeanList, long totalResults) {
        EventsResult eventResult;
        long numSyncEvents = 0;

        if (eventBeanList.size()==0) {
            eventResult = new EventsResult(APIResult.Status.SUCCEEDED, "Empty");
        } else {
            eventResult = new EventsResult(APIResult.Status.SUCCEEDED, "Success");
        }

        EventsResult.EventInstance[] events = new EventsResult.EventInstance[eventBeanList.size()];
        int index = 0;
        for (EventBean bean : eventBeanList) {
            EventsResult.EventInstance eventInstance = new EventsResult.EventInstance();
            if (StringUtils.isNotBlank(bean.getPolicyId())) {
                eventInstance.policyId = bean.getPolicyId();
            }
            if (StringUtils.isNotBlank(bean.getPolicyId())) {
                eventInstance.instanceId = bean.getInstanceId();
            }
            eventInstance.event = getEventName(bean.getEventId());
            eventInstance.eventType = bean.getEventEntityType();
            if (EventEntityType.POLICY.getName().equals(eventInstance.eventType)) {
                if (EventInfo.getEventInfo(bean.getEventInfo()).getSyncEvent()) {
                    eventInstance.syncEvent = true;
                    numSyncEvents++;
                }
            }
            eventInstance.severity = bean.getEventSeverity();
            eventInstance.timestamp = DateUtil.formatDate(bean.getEventTimeStamp());
            eventInstance.message = bean.getEventMessage();
            events[index++] = eventInstance;
        }
        eventResult.setCollection(events, totalResults, numSyncEvents);
        return eventResult;
    }

    private static EventsResult getEventsResult(List<EventBean> eventBeanList) {
        return getEventsResult(eventBeanList, eventBeanList.size());
    }

    private static EventsResult getEventsList(List<String> eventNameList) {
        EventsResult eventResult;
        if (eventNameList.size()==0) {
            eventResult = new EventsResult(APIResult.Status.SUCCEEDED, "Empty");
        } else {
            eventResult = new EventsResult(APIResult.Status.SUCCEEDED, "Success");
        }

        EventsResult.EventInstance[] events = new EventsResult.EventInstance[eventNameList.size()];
        int index = 0;
        for (String event : eventNameList) {
            EventsResult.EventInstance eventInstance = new EventsResult.EventInstance();
            eventInstance.event = event;
            events[index++] = eventInstance;
        }

        eventResult.setCollection(events);
        return eventResult;
    }

    private static Date getEndDate(String endStr) {
        Date endDate;
        if (StringUtils.isEmpty(endStr)) {
            endDate = new Date();
        } else {
            endDate = DateUtil.parseDate(endStr);
        }
        return endDate;
    }

    private static Date getStartDate(String startStr, Date end, final int frequency, final int resultsPage) {
        Date startDate;
        if (StringUtils.isEmpty(startStr)) {
            long startMillis = end.getTime();
            startMillis -= SECOND_IN_MILLIS*resultsPage*frequency;
            startDate = new Date(startMillis);
            if (startDate.after(end)) {
                LOG.warn("Calculated start date: {} crossed end date: {} setting it to entity start date", startDate,
                    end);
                startDate = end;
            }
        } else {
            startDate = DateUtil.parseDate(startStr);
        }

        return startDate;
    }

    private static int getPolicyFrequency(String policyId) {
        try {
            return PolicyPersistenceHelper.getActivePolicy(policyId).getFrequencyInSec();
        } catch (BeaconStoreException e) {
            throw BeaconWebException.newAPIException(e);
        }
    }

    static Events validateEventName(String eventName) {
        Events event = null;
        for (Events e : Events.values()) {
            if (e.getName().equals(eventName)) {
                event = e;
                break;
            }
        }
        return event;
    }

    static EventEntityType validateEventEntityType(String entityType) {
        EventEntityType eventEntityType = null;
        for (EventEntityType e : EventEntityType.values()) {
            if ((e.getName()).equals(entityType)) {
                eventEntityType = e;
                break;
            }
        }
        return eventEntityType;
    }

    private static String getEventName(int eventId) {
        return Events.getEvent(eventId).getName();
    }
}
