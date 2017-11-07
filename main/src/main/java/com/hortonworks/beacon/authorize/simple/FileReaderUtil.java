/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.authorize.simple;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains File Reader utility.
 */
public final class FileReaderUtil {
    private static Logger logger = LoggerFactory.getLogger(FileReaderUtil.class);
    private static boolean isDebugEnabled = logger.isDebugEnabled();

    private FileReaderUtil(){
    }
    public static List<String> readFile(InputStream policyStoreStream) throws IOException {
        if (isDebugEnabled) {
            logger.debug("==> FileReaderUtil readFile()");
        }
        List<String> list = new ArrayList<>();
        List<String> fileLines = IOUtils.readLines(policyStoreStream, StandardCharsets.UTF_8);
        if (fileLines != null) {
            for (String line : fileLines) {
                if ((!line.startsWith("#")) && Pattern.matches(".+;;.*;;.*;;.+", line)) {
                    list.add(line);
                }
            }
        }

        if (isDebugEnabled) {
            logger.debug("<== FileReaderUtil readFile()");
            logger.debug("Policies read :: " + list);
        }

        return list;
    }
}
