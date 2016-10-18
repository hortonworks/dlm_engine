package com.hortonworks.beacon.entity.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public final class DateUtil {
    private DateUtil() {
    }

    public static DateFormat getDateFormat() {
        SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return  isoFormat;
    }
}
