/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.log;

import org.slf4j.Logger;
import org.slf4j.Marker;

import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;

/**
 * Implemented Log method.
 */
public abstract class BeaconLogMethod implements Logger {
    private Logger logger;

    BeaconLogMethod(Logger logger) {
        this.logger = logger;
    }

    Logger getLogger() {
        return logger;
    }

    public abstract void log(BeaconLog.Level level, String msgTemplate, Object... params);

    public boolean isEnabled(BeaconLog.Level level) {
        boolean enabled;
        switch (level) {
            case INFO:
                enabled = logger.isInfoEnabled();
                break;
            case ERROR:
                enabled = logger.isErrorEnabled();
                break;
            case WARN:
                enabled = logger.isWarnEnabled();
                break;
            case DEBUG:
                enabled = logger.isDebugEnabled();
                break;
            case TRACE:
                enabled = logger.isTraceEnabled();
                break;
            default:
                throw new IllegalArgumentException(
                        ResourceBundleService.getService().getString(MessageCode.COMM_000020.name(), level.name()));
        }

        return enabled;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean isTraceEnabled() {
        return isEnabled(BeaconLog.Level.TRACE);
    }

    @Override
    public void trace(String s) {
        log(BeaconLog.Level.TRACE, s);
    }

    @Override
    public void trace(String s, Object o) {
        log(BeaconLog.Level.TRACE, s, o);
    }

    @Override
    public void trace(String s, Object o, Object o1) {
        log(BeaconLog.Level.TRACE, s, o, o1);
    }

    @Override
    public void trace(String s, Object... objects) {
        log(BeaconLog.Level.TRACE, s, objects);
    }

    @Override
    public void trace(String s, Throwable throwable) {
        log(BeaconLog.Level.TRACE, s, throwable);
    }


    @Override
    public boolean isDebugEnabled() {
        return isEnabled(BeaconLog.Level.DEBUG);
    }

    @Override
    public void debug(String s) {
        log(BeaconLog.Level.DEBUG, s);
    }

    @Override
    public void debug(String s, Object o) {
        log(BeaconLog.Level.DEBUG, s, o);
    }

    @Override
    public void debug(String s, Object o, Object o1) {
        log(BeaconLog.Level.DEBUG, s, o, o1);
    }

    @Override
    public void debug(String s, Object... objects) {
        log(BeaconLog.Level.DEBUG, s, objects);
    }

    @Override
    public void debug(String s, Throwable throwable) {
        log(BeaconLog.Level.DEBUG, s, throwable);
    }

    @Override
    public boolean isInfoEnabled() {
        return isEnabled(BeaconLog.Level.INFO);
    }

    @Override
    public void info(String s) {
        log(BeaconLog.Level.INFO,  s);
    }

    @Override
    public void info(String s, Object o) {
        log(BeaconLog.Level.INFO, s, o);
    }

    @Override
    public void info(String s, Object o, Object o1) {
        log(BeaconLog.Level.INFO, s, o, o1);
    }

    @Override
    public void info(String s, Object... objects) {
        log(BeaconLog.Level.INFO, s, objects);
    }

    @Override
    public void info(String s, Throwable throwable) {
        log(BeaconLog.Level.INFO, s, throwable);
    }

    @Override
    public boolean isWarnEnabled() {
        return isEnabled(BeaconLog.Level.WARN);
    }

    @Override
    public void warn(String s) {
        log(BeaconLog.Level.WARN, s);
    }

    @Override
    public void warn(String s, Object o) {
        log(BeaconLog.Level.WARN, s, o);
    }

    @Override
    public void warn(String s, Object... objects) {
        log(BeaconLog.Level.WARN, s, objects);
    }

    @Override
    public void warn(String s, Object o, Object o1) {
        log(BeaconLog.Level.WARN, s, o, o1);
    }

    @Override
    public void warn(String s, Throwable throwable) {
        log(BeaconLog.Level.WARN, s, throwable);
    }

    @Override
    public boolean isErrorEnabled() {
        return isEnabled(BeaconLog.Level.ERROR);
    }

    @Override
    public void error(String s) {
        log(BeaconLog.Level.ERROR,  s);
    }

    @Override
    public void error(String s, Object o) {
        log(BeaconLog.Level.ERROR, s, o);
    }

    @Override
    public void error(String s, Object o, Object o1) {
        log(BeaconLog.Level.ERROR, s, o, o1);
    }

    @Override
    public void error(String s, Object... objects) {
        log(BeaconLog.Level.ERROR, s, objects);
    }

    @Override
    public void error(String s, Throwable throwable) {
        log(BeaconLog.Level.ERROR, s, throwable);
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return false;
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return false;
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return false;
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return false;
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return false;
    }

    @Override
    public void info(Marker marker, String s) {

    }

    @Override
    public void info(Marker marker, String s, Object o) {

    }

    @Override
    public void info(Marker marker, String s, Object o, Object o1) {

    }

    @Override
    public void info(Marker marker, String s, Object... objects) {

    }

    @Override
    public void info(Marker marker, String s, Throwable throwable) {

    }

    @Override
    public void warn(Marker marker, String s) {

    }

    @Override
    public void warn(Marker marker, String s, Object o) {

    }

    @Override
    public void warn(Marker marker, String s, Object o, Object o1) {

    }

    @Override
    public void warn(Marker marker, String s, Object... objects) {

    }

    @Override
    public void warn(Marker marker, String s, Throwable throwable) {

    }

    @Override
    public void trace(Marker marker, String s) {

    }

    @Override
    public void trace(Marker marker, String s, Object o) {

    }

    @Override
    public void trace(Marker marker, String s, Object o, Object o1) {

    }

    @Override
    public void trace(Marker marker, String s, Object... objects) {

    }

    @Override
    public void trace(Marker marker, String s, Throwable throwable) {

    }

    @Override
    public void debug(Marker marker, String s) {
    }

    @Override
    public void debug(Marker marker, String s, Object o) {

    }

    @Override
    public void debug(Marker marker, String s, Object o, Object o1) {

    }

    @Override
    public void debug(Marker marker, String s, Object... objects) {

    }

    @Override
    public void debug(Marker marker, String s, Throwable throwable) {

    }

    @Override
    public void error(Marker marker, String s) {

    }

    @Override
    public void error(Marker marker, String s, Object o) {

    }

    @Override
    public void error(Marker marker, String s, Object o, Object o1) {

    }

    @Override
    public void error(Marker marker, String s, Object... objects) {

    }

    @Override
    public void error(Marker marker, String s, Throwable throwable) {

    }
}
