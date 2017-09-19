/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.metrics.util;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.hortonworks.beacon.metrics.Progress;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Type;

/**
 * Replication Metrics Deserializer class.
 */
public class ReplicationMetricsDeserializer implements JsonDeserializer<ReplicationMetrics> {

    @Override
    public ReplicationMetrics deserialize(JsonElement jsonElement, Type type,
                                          JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {

        final JsonObject jsonObject = jsonElement.getAsJsonObject();
        String jobId  = jsonObject.get("jobId").getAsString();
        String jobType = jsonObject.get("jobType").getAsString();
        ReplicationMetrics replicationMetrics = new ReplicationMetrics();
        replicationMetrics.setJobId(jobId);
        replicationMetrics.setJobType(ReplicationMetrics.JobType.valueOf(jobType));
        Progress progress = new Progress();
        final JsonObject progressJsonObject = jsonObject.get("progress").getAsJsonObject();

        long total = progressJsonObject.get("total").getAsLong();
        if (total!=0L) {
            progress.setTotal(total);
        }

        long completed = progressJsonObject.get("completed").getAsLong();
        if (completed!=0L) {
            progress.setCompleted(completed);
        }

        long failed = progressJsonObject.get("failed").getAsLong();
        if (failed!=0L) {
            progress.setFailed(failed);
        }

        long killed = progressJsonObject.get("killed").getAsLong();
        if (killed!=0L) {
            progress.setKilled(killed);
        }

        String unit = progressJsonObject.get("unit").getAsString();
        if (StringUtils.isNotBlank(unit)) {
            progress.setUnit(unit);
        }

        if (progressJsonObject.get("bytesCopied").getAsLong()!=0L) {
            progress.setBytesCopied(progressJsonObject.get("bytesCopied").getAsLong());
        }

        if (progressJsonObject.get("filesCopied").getAsLong()!=0L) {
            progress.setFilesCopied(progressJsonObject.get("filesCopied").getAsLong());
        }

        if (progressJsonObject.get("timeTaken").getAsLong()!=0L) {
            progress.setTimeTaken(progressJsonObject.get("timeTaken").getAsLong());
        }

        replicationMetrics.setProgress(progress);
        return replicationMetrics;
    }
}
