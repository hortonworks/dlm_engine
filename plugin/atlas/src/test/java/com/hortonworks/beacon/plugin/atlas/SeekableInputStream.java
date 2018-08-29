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

package com.hortonworks.beacon.plugin.atlas;


import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Simulates FSDataInputStream by providing implementation for Seekable and PositionReadble.
 * To be used only for tests.
 */
public class SeekableInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {

    public SeekableInputStream(byte[] buf) {
        super(buf);
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (mark != 0) {
            throw new IllegalStateException();
        }

        reset();
        long skipped = skip(pos);

        if (skipped != pos) {
            throw new IOException();
        }
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {

        if (position >= buf.length) {
            throw new IllegalArgumentException();
        }

        if (position + length > buf.length) {
            throw new IllegalArgumentException();
        }

        if (length > buffer.length) {
            throw new IllegalArgumentException();
        }

        System.arraycopy(buf, (int) position, buffer, offset, length);
        return length;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        read(position, buffer, 0, buffer.length);

    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        read(position, buffer, offset, length);
    }
}
