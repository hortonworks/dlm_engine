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
