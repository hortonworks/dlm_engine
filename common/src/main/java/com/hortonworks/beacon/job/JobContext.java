/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.job;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;

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
    private boolean recovery;
    private boolean performJobAfterRecovery;

    /**
     * JSON keys for instance job context.
     */
    private enum JobContextParam {
        INSTANCE_ID("instanceId"),
        OFFSET("offset"),
        INTERRUPT("interrupt"),
        CONTEXT("context"),
        RECOVERY("recovery"),
        PERFORMJOBAFTERRECOVERY("performJobAfterRecovery");

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

    public boolean isRecovery() {
        return recovery;
    }

    public void setRecovery(boolean recovery) {
        this.recovery = recovery;
    }

    public boolean isPerformJobAfterRecovery() {
        return performJobAfterRecovery;
    }

    public void setPerformJobAfterRecovery(boolean performJobAfterRecovery) {
        this.performJobAfterRecovery = performJobAfterRecovery;
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
                    throw new RuntimeException(
                            ResourceBundleService.getService()
                                    .getString(MessageCode.COMM_000001.name()));
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
        jsonObject.add(JobContextParam.RECOVERY.key, new JsonPrimitive(recovery));
        jsonObject.add(JobContextParam.PERFORMJOBAFTERRECOVERY.key, new JsonPrimitive(performJobAfterRecovery));

        return jsonObject.toString();
    }
}
