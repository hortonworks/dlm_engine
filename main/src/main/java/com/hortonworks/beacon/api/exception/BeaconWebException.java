/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;

/**
 * Exception for REST APIs.
 */
public class BeaconWebException extends WebApplicationException {

    private static final BeaconLog LOG = BeaconLog.getLog(BeaconWebException.class);

    public static BeaconWebException newAPIException(Throwable throwable) {
        return newAPIException(throwable, Response.Status.BAD_REQUEST);
    }

    public static BeaconWebException newAPIException(Throwable throwable, Response.Status status) {
        String message = getMessage(throwable);
        return newAPIException(message, status, throwable);
    }

    public static BeaconWebException newAPIException(String message, Response.Status status) {
        return newAPIException(message, status, (Throwable) null);
    }

    public static BeaconWebException newAPIException(String message, Response.Status status, Throwable rootCause,
                Object... objects) {
        if (rootCause instanceof BeaconWebException) {
            return (BeaconWebException) rootCause;
        }
        Response response = Response.status(status).entity(new APIResult(APIResult.Status.FAILED, message, objects))
                .type(MediaType.APPLICATION_JSON_TYPE).build();
        BeaconWebException bwe;
        if (rootCause != null) {
            bwe = new BeaconWebException(rootCause, response);
        } else {
            bwe = new BeaconWebException(response);
        }
        LOG.error(MessageCode.MAIN_000075.name(), ResourceBundleService.getService().getString(message, objects), bwe);
        return bwe;
    }

    public static BeaconWebException newAPIException(String message, Response.Status status, Object... parameters) {
        return newAPIException(message, status, (Throwable) null, parameters);
    }

    public static BeaconWebException newAPIException(String message, Throwable t, Object... parameters) {
        return newAPIException(message, Response.Status.BAD_REQUEST, t, parameters);
    }

    public static BeaconWebException newAPIException(String message, Object... parameters) {
        return newAPIException(message, Response.Status.BAD_REQUEST, parameters);
    }

    private static String getMessage(Throwable e) {
        if (e instanceof BeaconWebException) {
            return ((APIResult) ((BeaconWebException) e).getResponse().getEntity()).getMessage();
        }
        if (e.getMessage() == null) {
            return MessageCode.MAIN_000173.name();
        }
        return e.getCause() == null ? e.getMessage() : e.getMessage() + "\nCausedBy: " + e.getCause().getMessage();
    }

    public BeaconWebException(Response response) {
        super(response);
    }

    public BeaconWebException(Throwable e, Response response) {
        super(e, response);
    }
}
