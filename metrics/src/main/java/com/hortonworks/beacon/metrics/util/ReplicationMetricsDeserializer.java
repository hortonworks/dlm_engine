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

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.hortonworks.beacon.metrics.Progress;
import com.hortonworks.beacon.metrics.ReplicationMetrics;

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

        progress.setTotal(progressJsonObject.get("total").getAsLong());

        progress.setCompleted(progressJsonObject.get("completed").getAsLong());

        progress.setFailed(progressJsonObject.get("failed").getAsLong());

        progress.setKilled(progressJsonObject.get("killed").getAsLong());

        progress.setUnit(progressJsonObject.get("unit").getAsString());

        progress.setBytesCopied(progressJsonObject.get("bytesCopied").getAsLong());

        progress.setFilesCopied(progressJsonObject.get("filesCopied").getAsLong());

        progress.setTimeTaken(progressJsonObject.get("timeTaken").getAsLong());
        progress.setDirectoriesCopied(progressJsonObject.get("dirCopied").getAsLong());

        replicationMetrics.setProgress(progress);
        return replicationMetrics;
    }
}
