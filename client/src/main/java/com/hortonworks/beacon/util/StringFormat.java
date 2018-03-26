/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.util;

import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to format the {} with the parameters passed.
 */
public final class StringFormat {

    private static final Logger LOG = LoggerFactory.getLogger(StringFormat.class);
    private StringFormat() {
    }

    public static String format(String message, Object... objects) {
        if (objects != null) {
            for (Object object : objects) {
                try{
                    message = message.replaceFirst("\\{\\}", Matcher.quoteReplacement(object.toString()));
                } catch(Exception e){
                    LOG.error("Exception occurred in Pattern {}", object.toString(), e);
                    throw e;
                }
            }
        }
        return message;
    }
}
