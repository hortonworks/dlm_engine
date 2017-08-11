/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
