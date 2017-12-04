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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.util.StringFormat;

/**
 * Exception for REST APIs.
 */
public class BeaconWebException extends WebApplicationException {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconWebException.class);

    public static BeaconWebException newAPIException(Throwable throwable) {
        return newAPIException(throwable, Response.Status.BAD_REQUEST);
    }

    public static BeaconWebException newAPIException(Throwable throwable, Response.Status status) {
        String message = getMessage(throwable);
        return newAPIException(status, throwable, message);
    }

    public static BeaconWebException newAPIException(String message) {
        return newAPIException(message, Response.Status.BAD_REQUEST);
    }

    public static BeaconWebException newAPIException(String message, Response.Status status) {
        return newAPIException(status, (Throwable) null, message);
    }

    public static BeaconWebException newAPIException(Response.Status status, Throwable rootCause, String message,
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
        LOG.error("Throwing web exception: {}", message, bwe);
        return bwe;
    }

    public static BeaconWebException newAPIException(String message, Object... objects) {
        return newAPIException(StringFormat.format(message, objects), Response.Status.BAD_REQUEST);
    }

    public static BeaconWebException newAPIException(Response.Status status, String message, Object... objects) {
        return newAPIException(status, (Throwable) null, StringFormat.format(message, objects));
    }

    private static String getMessage(Throwable e) {
        if (e instanceof BeaconWebException) {
            return ((APIResult) ((BeaconWebException) e).getResponse().getEntity()).getMessage();
        }
        if (e.getMessage() == null) {
            return "Unhandled Exception";
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
