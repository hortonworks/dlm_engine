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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

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

        private static ThreadLocal<Info> tlLogInfo = new ThreadLocal<Info>() {
            @Override
            protected Info initialValue() {
                return new Info();
            }
        };

        static void reset() {
            BeaconLogParams.clearParams();
        }

        public static Info get() {
            return tlLogInfo.get();
        }

        public static void remove() {
            tlLogInfo.remove();
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
                throw new IllegalArgumentException(
                        ResourceBundleService.getService().getString(MessageCode.COMM_000016.name(), name));
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
                throw new IllegalArgumentException(
                        ResourceBundleService.getService().getString(MessageCode.COMM_000016.name(), name));
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
        log.setMsgPrefix(Info.get().createPrefix());
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
            String message;
            try {
                message = ResourceBundleService.getService().getString(msgTemplate, params);
            } catch (NoSuchElementException e) {
                message = EnumUtils.isValidEnum(MessageCode.class, msgTemplate)
                    ? MessageCode.valueOf(msgTemplate).getMsg() : msgTemplate;
                if (ArrayUtils.isNotEmpty(params)){
                    message = MessageFormat.format(message, params);
                }
            }
            String prefixMsg = getMsgPrefix() != null ? getMsgPrefix() : Info.get().getInfoPrefix();
            String threadId = Thread.currentThread().getName() + "-" + Thread.currentThread().getId();
            prefixMsg = (prefixMsg != null && prefixMsg.length() > 0) ? prefixMsg + " " : "";
            String msg = threadId + " " + prefixMsg + message;
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
                            ResourceBundleService.getService().getString(MessageCode.COMM_000020.name(), level.name()));
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
