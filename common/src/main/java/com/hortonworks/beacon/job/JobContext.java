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

package com.hortonworks.beacon.job;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Job related details exchanged between two jobs.
 */
public class JobContext implements Serializable {

    private static final String CONTEXT_SEPARATOR = ";";
    private static final String EQUALS = "=";

    private String jobInstanceId;
    private int offset;
    private AtomicBoolean shouldInterrupt;
    private Map<String, String> jobContextMap;

    /**
     * JSON keys for instance job context.
     */
    private enum JobContextParam {
        INSTANCE_ID("instanceId"),
        OFFSET("offset"),
        INTERRUPT("interrupt"),
        CONTEXT("context");

        private String key;

        JobContextParam(String key) {
            this.key = key;
        }
    }

    public String getJobInstanceId() {
        return jobInstanceId;
    }

    public void setJobInstanceId(String jobInstanceId) {
        this.jobInstanceId = jobInstanceId;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public AtomicBoolean shouldInterrupt() {
        return shouldInterrupt;
    }

    public void setShouldInterrupt(AtomicBoolean shouldInterrupt) {
        this.shouldInterrupt = shouldInterrupt;
    }

    public Map<String, String> getJobContextMap() {
        return jobContextMap;
    }

    public void setJobContextMap(Map<String, String> jobContextMap) {
        this.jobContextMap = jobContextMap;
    }


    public static JobContext parseJobContext(String contextData) {
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(contextData);
        JsonObject jsonObject = jsonElement.getAsJsonObject();

        JobContext jobContext = new JobContext();
        jobContext.setJobInstanceId(jsonObject.get(JobContextParam.INSTANCE_ID.key).getAsString());
        jobContext.setOffset(jsonObject.get(JobContextParam.OFFSET.key).getAsInt());
        jobContext.setShouldInterrupt(
                new AtomicBoolean(jsonObject.get(JobContextParam.INTERRUPT.key).getAsBoolean()));
        String contextStr = jsonObject.get(JobContextParam.CONTEXT.key).getAsString();
        Map<String, String> contextMap = new HashMap<>();
        for (String keyValue : contextStr.trim().split(CONTEXT_SEPARATOR)) {
            if (StringUtils.isNotBlank(keyValue)) {
                String[] pair = keyValue.split(EQUALS);
                if (pair.length == 2) {
                    contextMap.put(pair[0], pair[1]);
                } else {
                    throw new RuntimeException("invalid data found while loading the context.");
                }
            }
        }
        jobContext.setJobContextMap(contextMap);
        return jobContext;
    }

    @Override
    public String toString() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add(JobContextParam.INSTANCE_ID.key, new JsonPrimitive(jobInstanceId));
        jsonObject.add(JobContextParam.OFFSET.key, new JsonPrimitive(offset));
        jsonObject.add(JobContextParam.INTERRUPT.key, new JsonPrimitive(shouldInterrupt.get()));
        StringBuilder contextData = new StringBuilder();
        for (Map.Entry<String, String> entry : jobContextMap.entrySet()) {
            if (contextData.length() > 0) {
                contextData.append(CONTEXT_SEPARATOR);
            }
            contextData.append(entry.getKey()).append(EQUALS).append(entry.getValue());
        }
        jsonObject.add(JobContextParam.CONTEXT.key, new JsonPrimitive(contextData.toString()));

        return jsonObject.toString();
    }
}
