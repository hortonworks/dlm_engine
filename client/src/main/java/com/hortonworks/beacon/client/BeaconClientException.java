package com.hortonworks.beacon.client;


import com.hortonworks.beacon.client.resource.APIResult;
import com.sun.jersey.api.client.ClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Exception thrown by BeaconClient.
 * Reasons:
 *  Ultimate goal of switching from current CLI to spring shell based CLI
 *  Spring Shell doesn't work well with unchecked Exceptions
 *  The exception currently only gets surfaced in CLI, and in code existing catch clauses will still work.
 */
public class BeaconClientException extends RuntimeException{
    private int status;
    private static final int MB = 1024 * 1024;
    private static final Logger LOG = LoggerFactory.getLogger(BeaconClientException.class);

    public BeaconClientException(String msg) {
        super(msg);
    }

    public BeaconClientException(int status, String msg) {
        super(msg);
        this.status = status;
    }

    public BeaconClientException(Throwable e) {
        super(e);
    }

    public BeaconClientException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public static BeaconClientException fromReponse(ClientResponse clientResponse) {
        ClientResponse.Status status = clientResponse.getClientResponseStatus();
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
            } catch (Throwable e) {
                message = e.getMessage();
            }
        }
        BeaconClientException bce = new BeaconClientException(status.getStatusCode(), message);
        LOG.error("Throwing client exception " + bce, bce);
        return bce;
    }
}
