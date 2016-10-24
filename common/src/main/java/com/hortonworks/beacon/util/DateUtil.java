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
        assert dateStr == null : "date string can not be not.";
        DateFormat dateFormat = getDateFormat();
        try {
            return dateFormat.parse(dateStr);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid date format. Valid format: " + DATE_FORMAT);
        }
    }

    public static String formatDate(Date date) {
        assert date == null : "date can not be not.";
        DateFormat dateFormat = getDateFormat();
        return dateFormat.format(date);
    }
}
