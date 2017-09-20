/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.metrics;

/**
 * Replication Progress.
 */
public class Progress {
    private long total;
    private long completed;
    private long failed;
    private long killed;
    private long filesCopied;
    private long dirCopied;
    private long bytesCopied;
    private long timeTaken;
    private String unit;

    public Progress() {
    }

    Progress(long total, long completed, String unit) {
        this.total = total;
        this.completed = completed;
        this.unit = unit;
    }

    long getTotal() {
        return total;
    }

    long getCompleted() {
        return completed;
    }

    public long getFailed() {
        return failed;
    }

    public long getKilled() {
        return killed;
    }

    public String getUnit() {
        return unit;
    }

    long getFilesCopied() {
        return filesCopied;
    }

    long getBytesCopied() {
        return bytesCopied;
    }

    long getTimeTaken() {
        return timeTaken;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    private long getValue(Long value) {
        return value != null ? value : 0;
    }

    public void setCompleted(Long completed) {
        this.completed = getValue(completed);
    }

    public void setFailed(Long failed) {
        this.failed = getValue(failed);
    }

    public void setKilled(Long killed) {
        this.killed = getValue(killed);
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public void setFilesCopied(Long filesCopied) {
        this.filesCopied = getValue(filesCopied);
    }

    public void setBytesCopied(Long bytesCopied) {
        this.bytesCopied = getValue(bytesCopied);
    }

    public void setTimeTaken(Long timeTaken) {
        this.timeTaken = getValue(timeTaken);
    }

    public void setDirectoriesCopied(Long directoriesCopied) {
        this.dirCopied = getValue(directoriesCopied);
    }

    public long getDirectoriesCopied() {
        return dirCopied;
    }
}
