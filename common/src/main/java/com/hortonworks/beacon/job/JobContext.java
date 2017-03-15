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

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
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
        try {
            JSONObject jsonObject = new JSONObject(contextData);
            JobContext jobContext = new JobContext();
            jobContext.setJobInstanceId(jsonObject.getString(JobContextParam.INSTANCE_ID.key));
            jobContext.setOffset(jsonObject.getInt(JobContextParam.OFFSET.key));
            jobContext.setShouldInterrupt(new AtomicBoolean(jsonObject.getBoolean(JobContextParam.INTERRUPT.key)));
            String contextStr = jsonObject.getString(JobContextParam.CONTEXT.key);
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
        } catch (JSONException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(JobContextParam.INSTANCE_ID.key, jobInstanceId);
            jsonObject.put(JobContextParam.OFFSET.key, offset);
            jsonObject.put(JobContextParam.INTERRUPT.key, shouldInterrupt.get());
            StringBuilder contextData = new StringBuilder();
            for (Map.Entry<String, String> entry : jobContextMap.entrySet()) {
                if (contextData.length() > 0) {
                    contextData.append(CONTEXT_SEPARATOR);
                }
                contextData.append(entry.getKey()).append(EQUALS).append(entry.getValue());
            }
            jsonObject.put(JobContextParam.CONTEXT.key, contextData.toString());
        } catch (JSONException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return jsonObject.toString();
    }
}
