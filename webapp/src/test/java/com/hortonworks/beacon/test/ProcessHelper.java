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

package com.hortonworks.beacon.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launch beacon server as process.
 */
public final class ProcessHelper {

    private ProcessHelper() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(ProcessHelper.class);

    public static Process startNew(String optionsAsString, String mainClass, String extraClassPath,
                                   String[] arguments) throws Exception {
        ProcessBuilder processBuilder = createProcess(optionsAsString, mainClass, extraClassPath, arguments);
        Process process = processBuilder.start();
        LOG.info("Process started with arguments: {}", Arrays.toString(arguments));
        Thread.sleep(4000); //wait for the server to come up.
        return process;
    }

    private static ProcessBuilder createProcess(String optionsAsString, String mainClass, String extraClassPath,
                                                String[] arguments) {
        String jvm = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = extraClassPath + System.getProperty("java.class.path");
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
