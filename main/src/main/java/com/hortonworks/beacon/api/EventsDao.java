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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.result.EventsResult;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.entity.util.PolicyDao;
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
public final class EventsDao {

    private static final Logger LOG = LoggerFactory.getLogger(EventsDao.class);
    private static final long SECOND_IN_MILLIS = 1000L;
    private static final int DEFAULT_FREQUENCY_IN_SECOND = 300;
    private PolicyDao policyDao = new PolicyDao();

    EventsResult getEventsWithPolicyName(String policyName, String startDate, String endDate,
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

    EventsResult getEventsWithName(int eventId, String startDate, String endDate,
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

    EventsResult getEntityTypeEvents(String eventEntityType, String startDate, String endDate,
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

    EventsResult getInstanceEvents(String instanceId) {
        EventsExecutor eventExecutor = new EventsExecutor();
        List<EventBean> beanList = eventExecutor.getInstanceEvents(instanceId);

        return getEventsResult(beanList);
    }

    EventsResult getEventsWithPolicyActionId(String policyName, int actionid) {
        EventsExecutor eventExecutor = new EventsExecutor();
        List<EventBean> beanList = eventExecutor.getEventsWithPolicyActionId(policyName, actionid);

        return getEventsResult(beanList);
    }

    EventsResult getAllEventsInfo(String startDate, String endDate, String orderBy, String sortBy,
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

    EventsResult getSupportedEventDetails() {
        List<String> eventNameList = new ArrayList<>();
        for (Events events : Events.values()) {
            eventNameList.add(events.getName());
        }

        return getEventsList(eventNameList);
    }

    private EventsResult getEventsResult(List<EventBean> eventBeanList, long totalResults) {
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

    private EventsResult getEventsResult(List<EventBean> eventBeanList) {
        return getEventsResult(eventBeanList, eventBeanList.size());
    }

    private EventsResult getEventsList(List<String> eventNameList) {
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

    private Date getEndDate(String endStr) {
        Date endDate;
        if (StringUtils.isEmpty(endStr)) {
            endDate = new Date();
        } else {
            endDate = DateUtil.parseDate(endStr);
        }
        return endDate;
    }

    private Date getStartDate(String startStr, Date end, final int frequency, final int resultsPage) {
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

    private int getPolicyFrequency(String policyId) {
        try {
            return policyDao.getActivePolicy(policyId).getFrequencyInSec();
        } catch (BeaconStoreException e) {
            throw BeaconWebException.newAPIException(e);
        }
    }

    Events validateEventName(String eventName) {
        Events event = null;
        for (Events e : Events.values()) {
            if (e.getName().equals(eventName)) {
                event = e;
                break;
            }
        }
        return event;
    }

    EventEntityType validateEventEntityType(String entityType) {
        EventEntityType eventEntityType = null;
        for (EventEntityType e : EventEntityType.values()) {
            if ((e.getName()).equals(entityType)) {
                eventEntityType = e;
                break;
            }
        }
        return eventEntityType;
    }

    private String getEventName(int eventId) {
        return Events.getEvent(eventId).getName();
    }
}
