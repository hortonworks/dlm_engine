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

/**
 * Job related details exchanged between two jobs.
 */
public class JobContext implements Serializable {

    private static final String CONTEXT_SEPARATOR = ";";
    private static final String EQUALS = "=";

    private String jobInstanceId;
    private int offset;
    private AtomicBoolean shouldInterrupt = new AtomicBoolean(false);
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

    public JobContext() {
        //Default is set to true, so START or END does not need to set them explicitly.
        performJobAfterRecovery = true;
        jobContextMap = new HashMap<>();
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
                    throw new RuntimeException("Invalid data found while loading the context.");
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
