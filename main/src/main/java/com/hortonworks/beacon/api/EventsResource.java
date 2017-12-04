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
        } finally{
            BeaconLogUtils.deletePrefix();
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
        } finally{
            BeaconLogUtils.deletePrefix();
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
        } finally{
            BeaconLogUtils.deletePrefix();
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
        } finally{
            BeaconLogUtils.deletePrefix();
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
        } finally{
            BeaconLogUtils.deletePrefix();
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
        } finally{
            BeaconLogUtils.deletePrefix();
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
            return BeaconEventsHelper.getSupportedEventDetails();
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }


    private EventsResult getEventsWithPolicyName(String policyName, String startDate, String endDate,
                                                 String orderBy, String sortBy,
                                                 Integer offset, Integer resultsPage) throws BeaconException {
        try {
            return BeaconEventsHelper.getEventsWithPolicyName(policyName, startDate, endDate, orderBy, sortBy,
                    offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private EventsResult getEventsWithName(String eventName, String startStr, String endStr,
                                           String orderBy, String sortBy, Integer offset, Integer resultsPage)
            throws BeaconException {
        try {
            Events event = BeaconEventsHelper.validateEventName(eventName);
            if (event == null) {
                throw new BeaconException("Event name: {} is not supported", eventName);
            }

            LOG.debug("Events id: {} for event name: {}", event.getId(), eventName);
            return BeaconEventsHelper.getEventsWithName(event.getId(), startStr, endStr,
                    orderBy, sortBy,  offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private EventsResult getEntityTypeEvents(String entityType, String startStr, String endStr,
                                             String orderBy, String sortBy,
                                             Integer offset, Integer resultsPage) throws BeaconException {
        try {
            EventEntityType type = BeaconEventsHelper.validateEventEntityType(entityType);
            if (type != null) {
                LOG.debug("Find events for the entity type: {}", type.getName());
                return BeaconEventsHelper.getEntityTypeEvents(type.getName(), startStr, endStr,
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
            return BeaconEventsHelper.getInstanceEvents(instanceId);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private EventsResult getEventsWithPolicyActionId(String policyName, Integer actionId) throws BeaconException {
        try {
            return BeaconEventsHelper.getEventsWithPolicyActionId(policyName, actionId);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }


    private EventsResult getAllEventsInfo(String startStr, String endStr, String orderBy, String sortBy,
                                          Integer offset, Integer resultsPage) throws BeaconException {
        try {
            return BeaconEventsHelper.getAllEventsInfo(startStr, endStr, orderBy, sortBy, offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }
}
