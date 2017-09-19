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
