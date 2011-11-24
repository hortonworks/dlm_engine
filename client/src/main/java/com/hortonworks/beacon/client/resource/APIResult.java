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

package com.hortonworks.beacon.client.resource;

import com.hortonworks.beacon.util.StringFormat;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.StringWriter;

/**
 * APIResult is the output returned by all the APIs; status-SUCCEEDED or FAILED
 * message- detailed message.
 */
@XmlRootElement(name = "result")
@XmlAccessorType(XmlAccessType.FIELD)
public class APIResult {

    private Status status;

    private String message;

    private String entityId;

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
    public enum Status {
        SUCCEEDED, PARTIAL, FAILED
    }

    public APIResult(Status status, String message, Object...objects) {
        super();
        this.status = status;
        this.message = StringFormat.format(message, objects);
    }

    public APIResult(String entityId, Status status, String message, Object...objects) {
        this(status, message, objects);
        this.entityId = entityId;
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

    public String getEntityId() {
        return entityId;
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
