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
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.rb.MessageCode;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

/**
 * Beacon events resource management operations as REST API. Root resource (exposed at "myresource" path).
 */

@Path("/api/beacon/events")
public class EventsResource extends AbstractResourceManager {

    private static final BeaconLog LOG = BeaconLog.getLog(EventsResource.class);

    @GET
    @Path("policy/{policy_name}")
    @Produces({MediaType.APPLICATION_JSON})
    public EventsResult eventsWithPolicyName(@PathParam("policy_name") String policyName,
                                             @QueryParam("start") String startDate,
                                             @QueryParam("end") String endDate,
                                             @DefaultValue("eventTimeStamp") @QueryParam("orderBy") String orderBy,
                                             @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                             @DefaultValue("0") @QueryParam("offset") Integer offset,
                                             @QueryParam("numResults") Integer resultsPerPage,
                                             @Context HttpServletRequest request) {

        if (StringUtils.isBlank(policyName)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Policy name");
        }
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        List<String> keys = Arrays.asList("policyName", "start", "end", "orderBy", "sortBy", "offset", "numResults");
        List<String> values = Arrays.asList(policyName, startDate, endDate, orderBy, sortBy, offset.toString(),
                resultsPerPage.toString());
        LOG.info(MessageCode.MAIN_000167.name(), concatKeyValue(keys, values));
        try {
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return getEventsWithPolicyName(policyName, startDate, endDate, orderBy, sortBy,
                    offset, resultsPerPage);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
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
                                       @QueryParam("numResults") Integer resultsPerPage,
                                       @Context HttpServletRequest request) {
        if (StringUtils.isBlank(eventName)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Event Type");
        }
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        List<String> keys = Arrays.asList("eventName", "start", "end", "orderBy", "sortBy", "offset", "numResults");
        List<String> values = Arrays.asList(eventName, startStr, endStr, orderBy, sortBy, offset.toString(),
                resultsPerPage.toString());
        LOG.info(MessageCode.MAIN_000167.name(), concatKeyValue(keys, values));
        try {
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return getEventsWithName(eventName, startStr, endStr, orderBy, sortBy, offset, resultsPerPage);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
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
                                         @QueryParam("numResults") Integer resultsPerPage,
                                         @Context HttpServletRequest request) {
        if (StringUtils.isBlank(entityType)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Event Type");
        }
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        List<String> keys = Arrays.asList("entityType", "start", "end", "orderBy", "sortBy", "offset", "numResults");
        List<String> values = Arrays.asList(entityType, startStr, endStr, orderBy, sortBy, offset.toString(),
                resultsPerPage.toString());
        LOG.info(MessageCode.MAIN_000167.name(), concatKeyValue(keys, values));
        try {
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return getEntityTypeEvents(entityType, startStr, endStr, orderBy, sortBy, offset, resultsPerPage);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("instance")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult eventsForInstance(@QueryParam("instanceId") String instanceId,
                                          @Context HttpServletRequest request) {

        if (StringUtils.isBlank(instanceId)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Instance Id");
        }
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        try {
            return getEventsForInstance(instanceId);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("policy/{policy_name}/{action_id}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult eventsWithPolicyActionId(@PathParam("policy_name") String policyName,
                                                 @PathParam("action_id") Integer actionId,
                                                 @Context HttpServletRequest request) {

        if (StringUtils.isBlank(policyName)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Policy name");
        }
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        List<String> keys = Arrays.asList("policyName", "actionId");
        List<String> values = Arrays.asList(policyName, actionId.toString());
        LOG.info(MessageCode.MAIN_000167.name(), concatKeyValue(keys, values));
        try {
            return getEventsWithPolicyActionId(policyName, actionId);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
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
                                      @QueryParam("numResults") Integer resultsPerPage,
                                      @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        List<String> keys = Arrays.asList("start", "end", "orderBy", "sortBy", "offset", "numResults");
        List<String> values = Arrays.asList(startStr, endStr, orderBy, sortBy, offset.toString(),
                resultsPerPage.toString());
        LOG.info(MessageCode.MAIN_000167.name(), concatKeyValue(keys, values));
        try {
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return getAllEventsInfo(startStr, endStr, orderBy, sortBy, offset, resultsPerPage);
        }  catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
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
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
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
                throw new BeaconException(MessageCode.MAIN_000022.name(), eventName);
            }

            LOG.debug(MessageCode.MAIN_000056.name(), event.getId(), eventName);
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
                LOG.debug(MessageCode.MAIN_000057.name(), type.getName());
                return BeaconEventsHelper.getEntityTypeEvents(type.getName(), startStr, endStr,
                        orderBy, sortBy, offset, resultsPage);
            } else {
                throw new BeaconException(MessageCode.MAIN_000022.name(), entityType);
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