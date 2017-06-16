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

package com.hortonworks.beacon.api.exception;

import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.service.Services;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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

    public static BeaconWebException newAPIException(String message, Object... parameters) {
        try {
            message = ((ResourceBundleService) Services.get().getService(ResourceBundleService.get().getName()))
                    .getString(message, parameters);
        } catch (Exception e) {
            return newAPIException(message, Response.Status.BAD_REQUEST, e);
        }
        return newAPIException(message, Response.Status.BAD_REQUEST);
    }

    public static BeaconWebException newAPIException(String message, Response.Status status) {
        try {
            message = ((ResourceBundleService) Services.get().getService(ResourceBundleService.get().getName()))
                    .getString(message);
        } catch (Exception e) {
            return newAPIException(message, status, e);
        }
        return newAPIException(message, status, null);
    }

    public static BeaconWebException newAPIException(String message,
                                                     Response.Status status, Throwable rootCause) {
        Response response = Response.status(status)
                .entity(new APIResult(APIResult.Status.FAILED, message))
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
        BeaconWebException bwe;
        if (rootCause != null) {
            bwe = new BeaconWebException(rootCause, response);
        } else {
            bwe = new BeaconWebException(response);
        }
        LOG.error("Throwing web exception: {}", message, bwe);
        return bwe;
    }

    private static String getMessage(Throwable e) {
        if (e instanceof BeaconWebException) {
            return ((APIResult) ((BeaconWebException) e).getResponse().getEntity()).getMessage();
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
