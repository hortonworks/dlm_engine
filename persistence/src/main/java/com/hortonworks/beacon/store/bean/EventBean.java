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
        @NamedQuery(name="GET_EVENTS_FOR_POLICY", query="SELECT OBJECT(a) FROM EventBean a WHERE a.policyId = ("
                + "SELECT b.id FROM PolicyBean b "
                + "WHERE b.name=:policyName) "
                + "AND (a.eventTimeStamp BETWEEN :startTime AND :endTime) "
                + "ORDER BY a.eventTimeStamp DESC"),
        @NamedQuery(name="GET_EVENTS_FOR_INSTANCE_ID", query= "SELECT OBJECT(a) FROM EventBean a "
                + "WHERE a.instanceId=:instanceId"),
        @NamedQuery(name="GET_EVENTS_FOR_ENTITY_TYPE", query = "SELECT OBJECT(a) FROM EventBean a "
                + "WHERE a.eventEntityType=:eventEntityType "
                + "AND (a.eventTimeStamp BETWEEN :startTime AND :endTime) "
                + "ORDER BY a.eventTimeStamp DESC"),
        @NamedQuery(name="GET_ALL_EVENTS", query = "SELECT OBJECT(a) FROM EventBean a "
                + "WHERE a.eventTimeStamp BETWEEN :startTime AND :endTime "),
        @NamedQuery(name="GET_EVENTS_FOR_EVENT_ID", query="SELECT OBJECT(a) FROM EventBean a "
                + "WHERE a.eventId=:eventId "
                + "AND (a.eventTimeStamp BETWEEN :startTime AND :endTime) "
                + "ORDER BY a.eventTimeStamp DESC"),
        @NamedQuery(name="GET_EVENTS_FOR_EVENT_STATUS", query="SELECT OBJECT(a) FROM EventBean a "
                + "WHERE a.eventStatus=:eventStatus "
                + "AND (a.eventTimeStamp BETWEEN :startTime AND :endTime) "
                + "ORDER BY a.eventTimeStamp DESC"),
        @NamedQuery(name="GET_POLICY_ID", query="SELECT b.id FROM PolicyBean b WHERE b.name=:policyName"),
    })
public class EventBean {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column (name="id")
    private long id;

    @Column (name="policy_id")
    private String policyId;

    @Column (name="instance_id")
    private String instanceId;

    @Column (name="event_entity_type")
    private String eventEntityType;

    @Column (name="event_id")
    private int eventId;

    @Column (name="event_timestamp")
    private java.sql.Timestamp eventTimeStamp;

    @Column (name="event_status")
    private String eventStatus;

    @Column (name="event_message")
    private String eventMessage;

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

    public Timestamp getEventTimeStamp() {
        return new Timestamp(eventTimeStamp.getTime());
    }

    public void setEventTimeStamp(Timestamp eventTimeStamp) {
        this.eventTimeStamp = new Timestamp(eventTimeStamp.getTime());
    }

    public String getEventStatus() {
        return eventStatus;
    }

    public void setEventStatus(String eventStatus) {
        this.eventStatus = eventStatus;
    }

    public String getEventMessage() {
        return eventMessage;
    }

    public void setEventMessage(String eventMessage) {
        this.eventMessage = eventMessage;
    }
}
