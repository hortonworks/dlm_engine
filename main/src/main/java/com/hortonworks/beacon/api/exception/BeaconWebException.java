package com.hortonworks.beacon.api.exception;

import com.hortonworks.beacon.api.result.APIResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Exception for REST APIs.
 */
public class BeaconWebException extends WebApplicationException {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconWebException.class);

    public static BeaconWebException newMetadataResourceException(String message, Response.Status status) {
        LOG.error("Action failed: {}\nError: {}", status, message);
        // Using MediaType.TEXT_PLAIN for newMetadataResourceException to ensure backward compatibility.
        return new BeaconWebException(new Exception(message),
                Response.status(status).entity(message).type(MediaType.TEXT_PLAIN).build());
    }

    public static BeaconWebException newAPIException(Throwable throwable) {
        return newAPIException(throwable, Response.Status.BAD_REQUEST);
    }

    public static BeaconWebException newAPIException(Throwable throwable, Response.Status status) {
        String message = getMessage(throwable);
        return newAPIException(message, status);
    }

    public static BeaconWebException newAPIException(String message) {
        return newAPIException(message, Response.Status.BAD_REQUEST);
    }

    public static BeaconWebException newAPIException(String message, Response.Status status) {
        Response response = Response.status(status)
                .entity(new APIResult(APIResult.Status.FAILED, message))
                .type(MediaType.TEXT_XML_TYPE)
                .build();
        return new BeaconWebException(response);
    }

    private static String getMessage(Throwable e) {
        if (e instanceof BeaconWebException) {
            return ((APIResult)((BeaconWebException) e).getResponse().getEntity()).getMessage();
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
