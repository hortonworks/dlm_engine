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

package com.hortonworks.beacon.metrics.util;

import com.google.gson.Gson;
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
        return new Gson().fromJson(jsonString, ReplicationMetrics.class);
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
