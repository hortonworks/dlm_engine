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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Replication metrics utility class.
 */
public final class ReplicationMetricsUtils {
    private ReplicationMetricsUtils() {
    }

    public static ReplicationMetrics getReplicationMetrics(String jsonString) {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(ReplicationMetrics.class, new ReplicationMetricsDeserializer());
        Gson gson = gsonBuilder.create();
        return gson.fromJson(jsonString, ReplicationMetrics.class);
    }

    public static List<ReplicationMetrics> getListOfReplicationMetrics(String jsonString) {
        if (StringUtils.isBlank(jsonString)) {
            return new ArrayList<>();
        }
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(jsonString);
        if (element.isJsonArray()) {
            ReplicationMetrics[] metricsArray = new Gson().fromJson(jsonString, ReplicationMetrics[].class);
            return Arrays.asList(metricsArray);
        } else {
            return Arrays.asList(getReplicationMetrics(jsonString));
        }
    }

    public static String toJsonString(List<ReplicationMetrics> metricsList) {
        if (metricsList == null || metricsList.isEmpty()) {
            return null;
        }
        if (metricsList.size() > 1) {
            Type listType = new TypeToken<ArrayList<ReplicationMetrics>>() {
            }.getType();
            return new Gson().toJson(metricsList, listType);
        } else {
            return metricsList.get(0).toJsonString();
        }
    }

}
