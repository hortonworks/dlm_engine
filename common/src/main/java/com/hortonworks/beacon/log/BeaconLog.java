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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.util.StringFormat;

/**
 * Logger class for Beacon.
 */
public class BeaconLog extends BeaconLogMethod {

    private String prefix = null;

    /**
     * Beacon Log Levels.
     */
    public enum Level {
        ERROR, INFO, WARN, DEBUG, TRACE
    }

    public BeaconLog(Logger logger) {
        super(logger);
    }

    public static BeaconLog getLog(String name) {
        return new BeaconLog(LoggerFactory.getLogger(name));
    }

    public static BeaconLog getLog(Class clazz) {
        return new BeaconLog(LoggerFactory.getLogger(clazz));
    }

    /**
     * Beacon Log Info class to store contextual information to create log prefixes.
     */
    public static class Info {
        private String infoPrefix = "";

        static void reset() {
            BeaconLogParams.clearParams();
        }

        private Map<String, String> parameters = new HashMap<>();

        Info() {
        }

        public Info(Info logInfo) {
            setParameters(logInfo);
        }

        public void clear() {
            parameters.clear();
            resetPrefix();
        }

        public void setParameter(String name, String value) {
            if (!verifyParameterNames(name)) {
                throw new IllegalArgumentException("Parameter: " + name + " is not defined");
            }
            parameters.put(name, value);
        }

        private boolean verifyParameterNames(String name) {
            return BeaconLogParams.checkParams(name);
        }

        String getParameter(String name) {
            return parameters.get(name);
        }

        void clearParameter(String name) {
            if (!verifyParameterNames(name)) {
                throw new IllegalArgumentException("Parameter: " + name + " is not defined");
            }
            parameters.remove(name);
        }


        void setParameters(Info logInfo) {
            parameters.clear();
            parameters.putAll(logInfo.parameters);
        }

        String createPrefix() {
            StringBuilder sb = new StringBuilder();
            for (int i=0; i<BeaconLogParams.size(); i++){
                String paramName = BeaconLogParams.getParam(i);
                if (parameters.containsKey(paramName)
                        && StringUtils.isNotBlank(parameters.get(paramName))) {
                    sb.append(paramName);
                    sb.append("[");
                    sb.append(parameters.get(paramName));
                    sb.append("] ");
                }
            }

            return sb.toString().trim();
        }

        String resetPrefix() {
            infoPrefix = createPrefix();
            return infoPrefix;
        }

        String getInfoPrefix() {
            return infoPrefix;
        }
    }

    static BeaconLog resetPrefix(BeaconLog log) {
        log.setMsgPrefix(new Info().createPrefix());
        return log;
    }

    String getMsgPrefix() {
        return prefix;
    }

    void setMsgPrefix(String msgPrefix) {
        this.prefix = msgPrefix;
    }

    public void log(Level level, String msgTemplate, Object... params) {
        if (isEnabled(level)) {
            String prefixMsg = getMsgPrefix() != null ? getMsgPrefix() : new Info().getInfoPrefix();
            String threadId = Thread.currentThread().getName() + "-" + Thread.currentThread().getId();
            prefixMsg = (prefixMsg != null && prefixMsg.length() > 0) ? prefixMsg + " " : "";
            String msg = threadId + " " + prefixMsg + msgTemplate;
            Throwable throwable = getCause(params);
            Logger log = getLogger();
            switch (level) {
                case ERROR:
                    log.error(msg, throwable);
                    break;
                case INFO:
                    log.info(msg, throwable);
                    break;
                case WARN:
                    log.warn(msg, throwable);
                    break;
                case DEBUG:
                    log.debug(msg, throwable);
                    break;
                case TRACE:
                    log.trace(msg, throwable);
                    break;
                default:
                    throw new IllegalArgumentException(
                        StringFormat.format("Log level :{} is not supported", level.name()));
            }
        }
    }

    private static Throwable getCause(Object... params) {
        Throwable throwable = null;
        if (params != null && params.length > 0 && params[params.length - 1] instanceof Throwable) {
            throwable = (Throwable) params[params.length - 1];
        }
        return throwable;
    }
}
