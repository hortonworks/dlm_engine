/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.events;

import com.google.gson.Gson;

/**
 * Event Info class to handle custom attributes.
 */
public class EventInfo {
    private String sourceCluster;
    private String targetCluster;
    private String sourceDataset;
    private boolean syncEvent;

    String getSourceCluster() {
        return sourceCluster;
    }

    private void setSourceCluster(String sourceCluster) {
        this.sourceCluster = sourceCluster;
    }

    String getTargetCluster() {
        return targetCluster;
    }

    private void setTargetCluster(String targetCluster) {
        this.targetCluster = targetCluster;
    }

    String getSourceDataset() {
        return sourceDataset;
    }

    private void setSourceDataset(String sourceDataset) {
        this.sourceDataset = sourceDataset;
    }

    public boolean getSyncEvent() {
        return syncEvent;
    }

    private void setSyncEvent(boolean syncEvent) {
        this.syncEvent = syncEvent;
    }

    public static EventInfo getEventInfo(String eventInfo) {
        return new Gson().fromJson(eventInfo, EventInfo.class);
    }

    public String toJsonString() {
        return new Gson().toJson(this);
    }

    public void updateEventsInfo(String srcCluster, String tgtCluster,
                                 String srcDataset, boolean syncevent) {
        this.setSourceCluster(srcCluster);
        this.setTargetCluster(tgtCluster);
        this.setSourceDataset(srcDataset);
        this.setSyncEvent(syncevent);
    }
}
