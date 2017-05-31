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
