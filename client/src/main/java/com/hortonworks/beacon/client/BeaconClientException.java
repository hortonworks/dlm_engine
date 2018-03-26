/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client;


import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.StringFormat;
import com.sun.jersey.api.client.ClientResponse;

/**
 * Exception from beacon client which wraps any communication failure or API exception.
 */
public class BeaconClientException extends Exception {
    private int status;

    public BeaconClientException(String s, Throwable e) {
        super(s, e);
    }

    public BeaconClientException(int statusCode, APIResult result) {
        super(result.getMessage());
        this.status = statusCode;
    }

    public BeaconClientException(BeaconException e) {
        super(e);
    }

    public BeaconClientException(Throwable throwable, String msg, Object... objects) {
        super(StringFormat.format(msg, objects), throwable);
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public static BeaconClientException fromResponse(ClientResponse clientResponse) {
        ClientResponse.Status status = clientResponse.getClientResponseStatus();
        APIResult result = clientResponse.getEntity(APIResult.class);
        BeaconClientException bce = new BeaconClientException(status.getStatusCode(), result);
        return bce;
    }
}
