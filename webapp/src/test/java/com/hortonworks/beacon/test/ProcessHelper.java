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

package com.hortonworks.beacon.test;

import com.hortonworks.beacon.log.BeaconLog;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Launch beacon server as process.
 */
public final class ProcessHelper {

    private ProcessHelper() {
    }

    private static final BeaconLog LOG = BeaconLog.getLog(ProcessHelper.class);

    public static Process startNew(String optionsAsString, String mainClass, String[] arguments) throws Exception {
        ProcessBuilder processBuilder = createProcess(optionsAsString, mainClass, arguments);
        Process process = processBuilder.start();
        LOG.info("Process started with arguments: {0}", Arrays.toString(arguments));
        Thread.sleep(4000); //wait for the server to come up.
        return process;
    }

    private static ProcessBuilder createProcess(String optionsAsString, String mainClass, String[] arguments) {
        String jvm = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String[] options = optionsAsString.split(" ");
        List<String> command = new ArrayList<>();
        command.add(jvm);
        command.addAll(Arrays.asList(options));
        command.add("-cp");
        command.add(classpath);
        command.add(mainClass);
        command.addAll(Arrays.asList(arguments));

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.inheritIO();
        return processBuilder;
    }

    public static void killProcess(Process process) throws Exception {
        if (process != null) {
            process.destroy();
        }
    }
}
