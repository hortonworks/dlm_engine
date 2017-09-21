/**
 * Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 * <p>
 * Except as expressly permitted in a written agreement between you or your
 * company and Hortonworks, Inc. or an authorized affiliate or partner
 * thereof, any use, reproduction, modification, redistribution, sharing,
 * lending or other exploitation of all or any part of the contents of this
 * software is strictly prohibited.
 */


package com.hortonworks.beacon.client.cli;

import java.util.ListIterator;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;

public class IgnoreUnrecognisedOptionParser extends GnuParser {
    @Override
    protected void processOption(final String arg, final ListIterator iter) throws ParseException {
        if (getOptions().hasOption(arg)) {
            super.processOption(arg, iter);
        }
    }
}
