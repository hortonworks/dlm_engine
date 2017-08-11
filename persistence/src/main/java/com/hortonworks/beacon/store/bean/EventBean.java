/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.store.bean;


import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * Beacon Events Bean.
 */
@SuppressFBWarnings(value = {"NP_BOOLEAN_RETURN_NULL", "UWF_UNWRITTEN_FIELD"})
@Entity
@Table(name = "BEACON_EVENT")
@NamedQueries({
        @NamedQuery(name="GET_EVENTS_FOR_INSTANCE_ID", query= "SELECT OBJECT(a) FROM EventBean a "
                + "WHERE a.instanceId=:instanceId"),
        @NamedQuery(name="GET_POLICY_ID", query="SELECT b.id FROM PolicyBean b WHERE b.name=:policyName"),
    })
public class EventBean {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column (name = "id")
    private long id;

    @Column (name = "policy_id")
    private String policyId;

    @Column (name = "instance_id")
    private String instanceId;

    @Column (name = "event_entity_type")
    private String eventEntityType;

    @Column (name = "event_id")
    private int eventId;

    @Column (name = "event_severity")
    private String eventSeverity;

    @Column (name = "event_timestamp")
    private java.sql.Timestamp eventTimeStamp;

    @Column (name = "event_message")
    private String eventMessage;

    @Column (name = "event_info")
    private String eventInfo;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getEventEntityType() {
        return eventEntityType;
    }

    public void setEventEntityType(String eventEntityType) {
        this.eventEntityType = eventEntityType;
    }

    public int getEventId() {
        return eventId;
    }

    public void setEventId(int eventId) {
        this.eventId = eventId;
    }

    public String getEventSeverity() {
        return eventSeverity;
    }

    public void setEventSeverity(String eventSeverity) {
        this.eventSeverity = eventSeverity;
    }

    public Timestamp getEventTimeStamp() {
        return new Timestamp(eventTimeStamp.getTime());
    }

    public void setEventTimeStamp(Timestamp eventTimeStamp) {
        this.eventTimeStamp = new Timestamp(eventTimeStamp.getTime());
    }

    public String getEventMessage() {
        return eventMessage;
    }

    public void setEventMessage(String eventMessage) {
        this.eventMessage = eventMessage;
    }

    public String getEventInfo() {
        return eventInfo;
    }

    public void setEventInfo(String eventInfo) {
        this.eventInfo = eventInfo;
    }
}
