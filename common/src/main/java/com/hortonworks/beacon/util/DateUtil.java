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

package com.hortonworks.beacon.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public final class DateUtil {

    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

    private DateUtil() {
    }

    public static DateFormat getDateFormat() {
        SimpleDateFormat isoFormat = new SimpleDateFormat(DATE_FORMAT);
        isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return isoFormat;
    }

    public static Date parseDate(String dateStr) {
        if (dateStr == null) return null;
        DateFormat dateFormat = getDateFormat();
        try {
            return dateFormat.parse(dateStr);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid date format. Valid format: " + DATE_FORMAT);
        }
    }

    public static String formatDate(Date date) {
        if (date == null) return null;
        DateFormat dateFormat = getDateFormat();
        return dateFormat.format(date);
    }

    public static long getDateMillis(String date) {
        Date parseDate = parseDate(date);
        return parseDate.getTime();
    }

}
