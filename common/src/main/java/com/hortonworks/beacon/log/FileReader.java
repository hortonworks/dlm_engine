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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hortonworks.beacon.log.BeaconLogStreamer.ZIPFILE_EXTENSION;

/**
 * Constructs the FileReader with the given files.
 * The files will be read in the order given in the ArrayList.
 */
public class FileReader extends Reader {
    public static final Logger LOG = LoggerFactory.getLogger(FileReader.class);

    private File file;
    private Reader reader = null;

    FileReader(File file) {
        this.file = file;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (reader == null) {
            reader = openReader();
        }
        return reader.read(cbuf, off, len);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    private Reader openReader() throws IOException {
        LOG.debug("Reading file {}", file.getAbsolutePath());
        Reader localReader;
        // gzip files
        if (file.getName().endsWith(ZIPFILE_EXTENSION)) {
            GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(file));
            localReader = new InputStreamReader(gzipInputStream);
        } else {
            localReader = new java.io.FileReader(file);
        }
        return localReader;
    }
}
