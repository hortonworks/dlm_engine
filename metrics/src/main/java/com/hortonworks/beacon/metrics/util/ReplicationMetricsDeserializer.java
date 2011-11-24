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

        progress.setExportTotal(progressJsonObject.get("exportTotal").getAsLong());

        progress.setExportCompleted(progressJsonObject.get("exportCompleted").getAsLong());

        progress.setImportTotal(progressJsonObject.get("importTotal").getAsLong());

        progress.setImportCompleted(progressJsonObject.get("importCompleted").getAsLong());

        progress.setFailed(progressJsonObject.get("failed").getAsLong());

        progress.setKilled(progressJsonObject.get("killed").getAsLong());

        if (progressJsonObject.get("unit") != null) {
            progress.setUnit(progressJsonObject.get("unit").getAsString());
        }

        progress.setBytesCopied(progressJsonObject.get("bytesCopied").getAsLong());

        progress.setFilesCopied(progressJsonObject.get("filesCopied").getAsLong());

        progress.setTimeTaken(progressJsonObject.get("timeTaken").getAsLong());
        progress.setDirectoriesCopied(progressJsonObject.get("dirCopied").getAsLong());
        progress.setJobProgress(progressJsonObject.get("jobProgress").getAsLong());

        replicationMetrics.setProgress(progress);
        return replicationMetrics;
    }
}
