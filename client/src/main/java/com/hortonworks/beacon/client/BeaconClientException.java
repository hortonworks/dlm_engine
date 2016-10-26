package com.hortonworks.beacon.client;


import com.hortonworks.beacon.client.resource.APIResult;
import com.sun.jersey.api.client.ClientResponse;

import java.io.IOException;
import java.io.InputStream;

/**
 * Exception thrown by FalconClient.
 * This was converted to RuntimeException in FALCON-1609.
 * Reasons:
 *  Ultimate goal of switching from current CLI to spring shell based CLI
 *  Spring Shell doesn't work well with unchecked Exceptions
 *  The exception currently only gets surfaced in CLI, and in code existing catch clauses will still work.
 */
public class BeaconClientException extends RuntimeException{

    private static final int MB = 1024 * 1024;

    public BeaconClientException(String msg) {
        super(msg);
    }

    public BeaconClientException(Throwable e) {
        super(e);
    }

    public BeaconClientException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

    public static BeaconClientException fromReponse(ClientResponse clientResponse) {
        ClientResponse.Status status = clientResponse.getClientResponseStatus();
        String statusValue = status.toString();
        String message = "";
        clientResponse.bufferEntity();
        InputStream in = clientResponse.getEntityInputStream();
        try {
            in.mark(MB);
            message = clientResponse.getEntity(APIResult.class).getMessage();
        } catch (Throwable th) {
            byte[] data = new byte[MB];
            try {
                in.reset();
                int len = in.read(data);
                message = new String(data, 0, len);
            } catch (IOException e) {
                message = e.getMessage();
            }
        }
        return new BeaconClientException(statusValue + ";" + message);
    }
}
