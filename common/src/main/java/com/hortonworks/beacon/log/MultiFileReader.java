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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

/**
 * Constructs the MultiFileReader with the given files.
 * The files will be read in the order given in the ArrayList.
 */
public class MultiFileReader extends Reader {

    private ArrayList<File> files;
    private int index;
    private Reader reader;
    private boolean closed;

    MultiFileReader(ArrayList<File> files) throws IOException {
        this.files = files;
        closed = false;
        index = 0;
        reader = null;
        openNextReader();
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        int numRead = -1;
        while(!closed && numRead == -1) {
            numRead = reader.read(cbuf, off, len);
            if (numRead == -1) {
                reader.close();
                openNextReader();
            }
        }
        return numRead;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
        closed = true;
    }

    private void openNextReader() throws IOException {
        if (index < files.size()) {
            // gzip files
            if (files.get(index).getName().endsWith(".gz")) {
                GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(files.get(index)));
                reader = new InputStreamReader(gzipInputStream);
            } else {
                reader = new FileReader(files.get(index));
            }
            index++;
        } else {
            closed = true;
        }
    }
}
