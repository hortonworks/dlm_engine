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

package com.hortonworks.beacon.util;


import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;
import javax.servlet.jsp.el.FunctionMapper;
import javax.servlet.jsp.el.VariableResolver;

import org.apache.commons.el.ExpressionEvaluatorImpl;

import com.hortonworks.beacon.exceptions.BeaconException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * Helper for evaluating expressions.
 */
public final class ExpressionHelper implements FunctionMapper, VariableResolver {

    private static final ExpressionHelper INSTANCE = new ExpressionHelper();

    private static final ThreadLocal<Properties> THREAD_VARIABLES = new ThreadLocal<Properties>();

    private static final Pattern SYS_PROPERTY_PATTERN = Pattern.compile("\\$\\{[A-Za-z0-9_.]+\\}");

    private static final ExpressionEvaluator EVALUATOR = new ExpressionEvaluatorImpl();
    private static final ExpressionHelper RESOLVER = ExpressionHelper.get();


    public static final ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            return format;
        }
    };

    public static ExpressionHelper get() {
        return INSTANCE;
    }

    private enum DayOfWeek {
        SUN, MON, TUE, WED, THU, FRI, SAT
    }

    private ExpressionHelper() {
    }

    public <T> T evaluate(String expression, Class<T> clazz) throws BeaconException {
        return evaluateFullExpression("${" + expression + "}", clazz);
    }

    @SuppressWarnings("unchecked")
    public <T> T evaluateFullExpression(String expression, Class<T> clazz) throws BeaconException {
        try {
            return (T) EVALUATOR.evaluate(expression, clazz, RESOLVER, RESOLVER);
        } catch (ELException e) {
            throw new BeaconException(e, "Unable to evaluate {}", expression);
        }
    }

    @Override
    public Method resolveFunction(String prefix, String name) {
        for (Method method : ExpressionHelper.class.getDeclaredMethods()) {
            if (method.getName().equals(name)) {
                return method;
            }
        }
        throw new UnsupportedOperationException(StringFormat.format("Function not found {}: {}", prefix, name));
    }

    public void setPropertiesForVariable(Properties properties) {
        THREAD_VARIABLES.set(properties);
    }

    @Override
    public Object resolveVariable(String field) {
        return THREAD_VARIABLES.get().get(field);
    }

    private static ThreadLocal<Date> referenceDate = new ThreadLocal<Date>();


    private static int getDayOffset(String weekDayName) {
        int day;
        Calendar nominalTime = Calendar.getInstance();
        nominalTime.setTimeZone(TimeZone.getTimeZone("UTC"));
        nominalTime.setTime(referenceDate.get());
        int currentWeekDay = nominalTime.get(Calendar.DAY_OF_WEEK);
        int weekDay = DayOfWeek.valueOf(weekDayName).ordinal() + 1; //to map to Calendar.SUNDAY ...
        day = weekDay - currentWeekDay;
        if (weekDay > currentWeekDay) {
            day = day - 7;
        }
        return day;
    }

    @SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
    private static Date getRelative(Date date, int boundary, int month, int day, int hour, int minute) {
        Calendar dsInstanceCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        dsInstanceCal.setTime(date);
        switch (boundary) {
            case Calendar.YEAR:
                dsInstanceCal.set(Calendar.MONTH, 0);
            case Calendar.MONTH:
                dsInstanceCal.set(Calendar.DAY_OF_MONTH, 1);
            case Calendar.DAY_OF_MONTH:
                dsInstanceCal.set(Calendar.HOUR_OF_DAY, 0);
            case Calendar.HOUR:
                dsInstanceCal.set(Calendar.MINUTE, 0);
                dsInstanceCal.set(Calendar.SECOND, 0);
                dsInstanceCal.set(Calendar.MILLISECOND, 0);
                break;
            case Calendar.SECOND:
                break;
            default:
                throw new IllegalArgumentException("Invalid boundary " + boundary);
        }

        dsInstanceCal.add(Calendar.YEAR, 0);
        dsInstanceCal.add(Calendar.MONTH, month);
        dsInstanceCal.add(Calendar.DAY_OF_MONTH, day);
        dsInstanceCal.add(Calendar.HOUR_OF_DAY, hour);
        dsInstanceCal.add(Calendar.MINUTE, minute);
        return dsInstanceCal.getTime();
    }

    public static Date now(int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.SECOND, 0, 0, hour, minute);
    }

    public static Date today(int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.DAY_OF_MONTH, 0, 0, hour, minute);
    }

    public static Date yesterday(int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.DAY_OF_MONTH, 0, -1, hour, minute);
    }

    public static Date currentMonth(int day, int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.MONTH, 0, day, hour, minute);
    }

    public static Date lastMonth(int day, int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.MONTH, -1, day, hour, minute);
    }

    public static Date currentWeek(String weekDay, int hour, int minute) {
        int day = getDayOffset(weekDay);
        return getRelative(referenceDate.get(), Calendar.DAY_OF_MONTH, 0, day, hour, minute);
    }

    public static Date lastWeek(String weekDay, int hour, int minute) {
        int day = getDayOffset(weekDay);
        return getRelative(referenceDate.get(), Calendar.DAY_OF_MONTH, 0, day - 7, hour, minute);
    }

    public static Date currentYear(int month, int day, int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.YEAR, month, day, hour, minute);
    }

    public static Date lastYear(int month, int day, int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.YEAR, month - 12, day, hour, minute);
    }

    public static Date latest(int n) {
        return referenceDate.get();
    }

    public static Date future(int n, int limit) {
        return referenceDate.get();
    }

    public static long hours(int val) {
        return TimeUnit.HOURS.toMillis(val);
    }

    public static long minutes(int val) {
        return TimeUnit.MINUTES.toMillis(val);
    }

    public static long days(int val) {
        return TimeUnit.DAYS.toMillis(val);
    }

    public static long months(int val) {
        return val * days(31);
    }

    public static long years(int val) {
        return val * days(366);
    }

    public static String substitute(String originalValue) {
        return substitute(originalValue, System.getProperties());
    }

    public static String substitute(String originalValue, Properties properties) {
        Matcher envVarMatcher = SYS_PROPERTY_PATTERN.matcher(originalValue);
        while (envVarMatcher.find()) {
            String envVar = originalValue.substring(envVarMatcher.start() + 2,
                    envVarMatcher.end() - 1);
            String envVal = properties.getProperty(envVar, System.getenv(envVar));

            envVar = "\\$\\{" + envVar + "\\}";
            if (envVal != null) {
                originalValue = originalValue.replaceAll(envVar, Matcher.quoteReplacement(envVal));
                envVarMatcher = SYS_PROPERTY_PATTERN.matcher(originalValue);
            }
        }
        return originalValue;
    }


    public static String user() {
        return "${user.name}";
    }

}
