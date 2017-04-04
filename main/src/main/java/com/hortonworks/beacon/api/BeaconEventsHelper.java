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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.api.result.EventsResult;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.EventStatus;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.exceptions.BeaconException;
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

    public static EventsResult getEventsWithPolicyName(String policyName, String startDate, String endDate,
                                                       int offset, int resultsPage) {
        EventsExecutor eventExecutor = new EventsExecutor();
        int frequency = getPolicyFrequency(policyName);
        Date endDateTime = getEndDate(endDate);
        Date startDateTime = getStartDate(startDate, endDateTime, frequency, resultsPage);

        List<EventBean> beanList = eventExecutor.getEventsWithPolicyName(policyName,
                startDateTime, endDateTime, offset, resultsPage);

        EventsResult eventList = getEventsResult(beanList);

        return eventList;
    }

    public static EventsResult getEventsWithName(int eventId, String startDate, String endDate,
                                                 Integer offset, Integer resultsPage) {
        EventsExecutor eventExecutor = new EventsExecutor();
        Date endDateTime = getEndDate(endDate);
        Date startDateTime = getStartDate(startDate, endDateTime, DEFAULT_FREQUENCY_IN_SECOND, resultsPage);
        List<EventBean> beanList = eventExecutor.getEventsWithName(eventId,
                startDateTime, endDateTime, offset, resultsPage);

        EventsResult eventList = getEventsResult(beanList);
        return eventList;
    }

    public static EventsResult getEntityTypeEvents(String eventEntityType, String startDate, String endDate,
                                                   Integer offset, Integer resultsPage) {
        LOG.info("Get events for type : {}", eventEntityType);
        EventsExecutor eventExecutor = new EventsExecutor();
        Date endDateTime = getEndDate(endDate);
        Date startDateTime = getStartDate(startDate, endDateTime, DEFAULT_FREQUENCY_IN_SECOND, resultsPage);
        List<EventBean> beanList = eventExecutor.getEntityTypeEvents(eventEntityType,
                startDateTime, endDateTime, offset, resultsPage);

        EventsResult eventList = getEventsResult(beanList);
        return eventList;
    }

    public static EventsResult getInstanceEvents(String instanceId) {
        EventsExecutor eventExecutor = new EventsExecutor();
        List<EventBean> beanList = eventExecutor.getInstanceEvents(instanceId);
        EventsResult eventList = getEventsResult(beanList);

        return eventList;
    }


    public static EventsResult getEventsWithStatus(String eventStatus, String startDate, String endDate,
                                                   int offset, int resultsPage) {
        EventsExecutor eventExecutor = new EventsExecutor();
        Date endDateTime = getEndDate(endDate);
        Date startDateTime = getStartDate(startDate, endDateTime, DEFAULT_FREQUENCY_IN_SECOND, resultsPage);

        List<EventBean> beanList = eventExecutor.getEventStatus(eventStatus,
                startDateTime, endDateTime, offset, resultsPage);

        EventsResult eventList = getEventsResult(beanList);
        return eventList;
    }

    public static EventsResult getEventsWithPolicyActionId(String policyName, int actionid) {
        EventsExecutor eventExecutor = new EventsExecutor();
        List<EventBean> beanList = eventExecutor.getEventsWithPolicyActionId(policyName, actionid);

        EventsResult eventList = getEventsResult(beanList);
        return eventList;
    }

    public static EventsResult getAllEventsInfo(String startDate, String endDate,
                                            Integer offset, Integer resultsPage) {
        EventsExecutor eventExecutor = new EventsExecutor();
        Date endDateTime = getEndDate(endDate);
        Date startDateTime = getStartDate(startDate, endDateTime, DEFAULT_FREQUENCY_IN_SECOND, resultsPage);
        List<EventBean> beanList = eventExecutor.getAllEventsInfo(startDateTime, endDateTime,
                offset, resultsPage);

        EventsResult eventList = getEventsResult(beanList);
        return eventList;
    }

    public static EventsResult getSupportedEventDetails() {
        List<String> eventNameList = new ArrayList<>();
        for (Events events : Events.values()) {
            eventNameList.add(events.getName());
        }

        EventsResult eventList = getEventsList(eventNameList);
        return eventList;
    }

    private static EventsResult getEventsResult(List<EventBean> eventBeanList) {
        EventsResult eventResult;

        if (eventBeanList.size()==0) {
            eventResult = new EventsResult(APIResult.Status.SUCCEEDED, "empty");
        } else {
            eventResult = new EventsResult(APIResult.Status.SUCCEEDED, "success");
        }

        EventsResult.EventInstance[] events = new EventsResult.EventInstance[eventBeanList.size()];
        int index = 0;
        for (EventBean bean : eventBeanList) {
            EventsResult.EventInstance eventInstance = new EventsResult.EventInstance();
            eventInstance.policyId = bean.getPolicyId();
            eventInstance.instanceId = bean.getInstanceId();
            eventInstance.event = getEventName(bean.getEventId());
            eventInstance.timestamp = bean.getEventTimeStamp().toString();
            eventInstance.eventStatus = bean.getEventStatus();
            eventInstance.message = bean.getEventMessage();
            events[index++] = eventInstance;
        }

        eventResult.setCollection(events);
        return eventResult;
    }

    private static EventsResult getEventsList(List<String> eventNameList) {
        EventsResult eventResult;
        if (eventNameList.size()==0) {
            eventResult = new EventsResult(APIResult.Status.SUCCEEDED, "empty");
        } else {
            eventResult = new EventsResult(APIResult.Status.SUCCEEDED, "success");
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
                LOG.warn("Calculated start date : {} crossed end date : {} setting it to "
                        + "entity start date", startDate, end);
                startDate = end;
            }
        } else {
            startDate = DateUtil.parseDate(startStr);
        }

        return startDate;
    }

    private static int getPolicyFrequency(String policyId) {
        try {
            return PersistenceHelper.getActivePolicy(policyId).getFrequencyInSec();
        } catch (BeaconStoreException e) {
            throw BeaconWebException.newAPIException(e);
        }
    }

    protected static Events validateEventName(String eventName) {
        Events event = null;
        for (Events e : Events.values()) {
            if (e.getName().equals(eventName)) {
                event = e;
                break;
            }
        }
        return event;
    }

    protected static EventEntityType validateEventEntityType(String entityType) {
        EventEntityType eventEntityType = null;
        for (EventEntityType e : EventEntityType.values()) {
            if ((e.getName()).equals(entityType)) {
                eventEntityType = e;
                break;
            }
        }
        return eventEntityType;
    }

    protected static EventStatus validateEventStatus(String eventStatusStr) throws BeaconException {
        EventStatus eventStatus = null;
        for(EventStatus status : EventStatus.values()) {
            if ((status.getName()).equals(eventStatusStr)) {
                eventStatus = status;
                break;
            }
        }
        return eventStatus;
    }

    private static String getEventName(int eventId) {
        return Events.getEvent(eventId).getName();
    }
}
