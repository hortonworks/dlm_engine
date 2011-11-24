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
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLogUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * Beacon events resource management operations as REST API. Root resource (exposed at "myresource" path).
 */

@Path("/api/beacon/events")
public class EventsResource extends AbstractResourceManager {

    private static final Logger LOG = LoggerFactory.getLogger(EventsResource.class);

    @GET
    @Path("policy/{policy_name}")
    @Produces({MediaType.APPLICATION_JSON})
    public EventsResult eventsWithPolicyName(@PathParam("policy_name") String policyName,
                                             @QueryParam("start") String startDate,
                                             @QueryParam("end") String endDate,
                                             @DefaultValue("eventTimeStamp") @QueryParam("orderBy") String orderBy,
                                             @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                             @DefaultValue("0") @QueryParam("offset") Integer offset,
                                             @QueryParam("numResults") Integer resultsPerPage) {

        if (StringUtils.isBlank(policyName)) {
            throw BeaconWebException.newAPIException("Policy name cannot be null or empty");
        }
        BeaconLogUtils.prefixPolicy(policyName);
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        try {
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return getEventsWithPolicyName(policyName, startDate, endDate, orderBy, sortBy,
                    offset, resultsPerPage);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("{event_name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult eventsWithName(@PathParam("event_name") String eventName,
                                       @QueryParam("start") String startStr,
                                       @QueryParam("end") String endStr,
                                       @DefaultValue("eventTimeStamp") @QueryParam("orderBy") String orderBy,
                                       @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                       @DefaultValue("0") @QueryParam("offset") Integer offset,
                                       @QueryParam("numResults") Integer resultsPerPage) {
        if (StringUtils.isBlank(eventName)) {
            throw BeaconWebException.newAPIException("Event Type cannot be null or empty");
        }
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        try {
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return getEventsWithName(eventName, startStr, endStr, orderBy, sortBy, offset, resultsPerPage);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("entity/{entity_type}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult entityTypeEvents(@PathParam("entity_type") String entityType,
                                         @QueryParam("start") String startStr,
                                         @QueryParam("end") String endStr,
                                         @DefaultValue("eventTimeStamp") @QueryParam("orderBy") String orderBy,
                                         @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                         @DefaultValue("0") @QueryParam("offset") Integer offset,
                                         @QueryParam("numResults") Integer resultsPerPage) {
        if (StringUtils.isBlank(entityType)) {
            throw BeaconWebException.newAPIException("Event Type cannot be null or empty");
        }
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        try {
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return getEntityTypeEvents(entityType, startStr, endStr, orderBy, sortBy, offset, resultsPerPage);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("instance")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult eventsForInstance(@QueryParam("instanceId") String instanceId) {

        if (StringUtils.isBlank(instanceId)) {
            throw BeaconWebException.newAPIException("Instance Id cannot be null or empty");
        }
        try {
            return getEventsForInstance(instanceId);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("policy/{policy_name}/{action_id}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult eventsWithPolicyActionId(@PathParam("policy_name") String policyName,
                                                 @PathParam("action_id") Integer actionId) {

        if (StringUtils.isBlank(policyName)) {
            throw BeaconWebException.newAPIException("Policy name cannot be null or empty");
        }
        BeaconLogUtils.prefixPolicy(policyName);
        try {
            return getEventsWithPolicyActionId(policyName, actionId);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }


    @GET
    @Path("all")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult allEventsInfo(@QueryParam("start") String startStr,
                                      @QueryParam("end") String endStr,
                                      @DefaultValue("eventTimeStamp") @QueryParam("orderBy") String orderBy,
                                      @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                      @DefaultValue("0") @QueryParam("offset") Integer offset,
                                      @QueryParam("numResults") Integer resultsPerPage) {
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        try {
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return getAllEventsInfo(startStr, endStr, orderBy, sortBy, offset, resultsPerPage);
        }  catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("/")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult supportedEventDetails() {
        try {
            return getSupportedEventDetails();
        }  catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    private EventsResult getSupportedEventDetails() throws BeaconException {
        try {
            return eventsDao.getSupportedEventDetails();
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }


    private EventsResult getEventsWithPolicyName(String policyName, String startDate, String endDate,
                                                 String orderBy, String sortBy,
                                                 Integer offset, Integer resultsPage) throws BeaconException {
        try {
            return eventsDao.getEventsWithPolicyName(policyName, startDate, endDate, orderBy, sortBy,
                    offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private EventsResult getEventsWithName(String eventName, String startStr, String endStr,
                                           String orderBy, String sortBy, Integer offset, Integer resultsPage)
            throws BeaconException {
        try {
            Events event = eventsDao.validateEventName(eventName);
            if (event == null) {
                throw new BeaconException("Event name: {} is not supported", eventName);
            }

            LOG.debug("Events id: {} for event name: {}", event.getId(), eventName);
            return eventsDao.getEventsWithName(event.getId(), startStr, endStr,
                    orderBy, sortBy,  offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private EventsResult getEntityTypeEvents(String entityType, String startStr, String endStr,
                                             String orderBy, String sortBy,
                                             Integer offset, Integer resultsPage) throws BeaconException {
        try {
            EventEntityType type = eventsDao.validateEventEntityType(entityType);
            if (type != null) {
                LOG.debug("Find events for the entity type: {}", type.getName());
                return eventsDao.getEntityTypeEvents(type.getName(), startStr, endStr,
                        orderBy, sortBy, offset, resultsPage);
            } else {
                throw new BeaconException("Event name: {} is not supported", entityType);
            }
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private EventsResult getEventsForInstance(String instanceId) throws BeaconException {
        try {
            return eventsDao.getInstanceEvents(instanceId);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private EventsResult getEventsWithPolicyActionId(String policyName, Integer actionId) throws BeaconException {
        try {
            return eventsDao.getEventsWithPolicyActionId(policyName, actionId);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }


    private EventsResult getAllEventsInfo(String startStr, String endStr, String orderBy, String sortBy,
                                          Integer offset, Integer resultsPage) throws BeaconException {
        try {
            return eventsDao.getAllEventsInfo(startStr, endStr, orderBy, sortBy, offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }
}
