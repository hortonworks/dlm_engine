package com.hortonworks.beacon.api.result;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.StringWriter;

@XmlRootElement(name = "result")
@XmlAccessorType(XmlAccessType.FIELD)
public class APIResult {

    private Status status;

    private String message;

    private String requestId;

    private static final JAXBContext JAXB_CONTEXT;

    static {
        try {
            JAXB_CONTEXT = JAXBContext.newInstance(APIResult.class);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * API Result status.
     */
    public static enum Status {
        SUCCEEDED, PARTIAL, FAILED
    }

    public APIResult(Status status, String message) {
        super();
        this.status = status;
        this.message = message;
        requestId = Thread.currentThread().getName();
    }

    protected APIResult() {
        // private default constructor for JAXB
    }

    public Status getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String reqId) {
        this.requestId = reqId;
    }

    @Override
    public String toString() {
        try {
            StringWriter stringWriter = new StringWriter();
            Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
            marshaller.marshal(this, stringWriter);
            return stringWriter.toString();
        } catch (JAXBException e) {
            return e.getMessage();
        }
    }

    public Object[] getCollection() {
        return null;
    }

    public void setCollection(Object[] items) {
    }
}
