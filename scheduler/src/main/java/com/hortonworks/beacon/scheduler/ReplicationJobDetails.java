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

package com.hortonworks.beacon.scheduler;


public class ReplicationJobDetails {

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public String getSrcHS2URL() {
        return srcHS2URL;
    }

    public void setSrcHS2URL(String srcHS2URL) {
        this.srcHS2URL = srcHS2URL;
    }
    public String getTargetHS2URL() {
        return targetHS2URL;
    }

    public void setTargetHS2URL(String targetHSURL) {
        this.targetHS2URL = targetHS2URL;
    }

    private String name;
    private int frequency;
    private String srcHS2URL;
    private String targetHS2URL;

    public ReplicationJobDetails(String name, int frequency, String srcHS2URL, String targetHS2URL) {
        this.name = name;
        this.frequency = frequency;
        this.srcHS2URL = srcHS2URL;
        this.targetHS2URL = targetHS2URL;
    }
}
