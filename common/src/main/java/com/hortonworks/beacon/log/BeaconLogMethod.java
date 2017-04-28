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

package com.hortonworks.beacon.log;

import org.slf4j.Logger;
import org.slf4j.Marker;

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
                throw new IllegalArgumentException("Log level :"+level.name()+" not supported");
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
