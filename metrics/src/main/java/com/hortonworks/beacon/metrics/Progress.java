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

package com.hortonworks.beacon.metrics;

/**
 * Replication Progress.
 */
public class Progress {
    private long total;
    private long completed;
    private long exportTotal;
    private long exportCompleted;
    private long importTotal;
    private long importCompleted;
    private long failed;
    private long killed;
    private long filesCopied;
    private long dirCopied;
    private long bytesCopied;
    private long timeTaken;
    private String unit;
    private float jobProgress;

    public Progress() {
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

    public void setCompleted(long completed) {
        this.completed = completed;
    }

    public void setFailed(long failed) {
        this.failed = failed;
    }

    public void setKilled(long killed) {
        this.killed = killed;
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

    public float getJobProgress() {
        return jobProgress;
    }

    public long getExportTotal() {
        return exportTotal;
    }

    public void setExportTotal(long exportTotal) {
        this.exportTotal = getValue(exportTotal);
    }

    public long getExportCompleted() {
        return exportCompleted;
    }

    public void setExportCompleted(long exportCompleted) {
        this.exportCompleted = getValue(exportCompleted);
    }

    public long getImportTotal() {
        return importTotal;
    }

    public void setImportTotal(long importTotal) {
        this.importTotal = getValue(importTotal);
    }

    public long getImportCompleted() {
        return importCompleted;
    }

    public void setImportCompleted(long importCompleted) {
        this.importCompleted = getValue(importCompleted);
    }

    public void setJobProgress(float jobProgress) {
        this.jobProgress = jobProgress;
    }

    @Override
    public String toString() {
        return "Progress{"
                + "total=" + total
                + ", completed=" + completed
                + ", exportTotal=" + exportTotal
                + ", exportCompleted=" + exportCompleted
                + ", importTotal=" + importTotal
                + ", importCompleted=" + importCompleted
                + ", failed=" + failed
                + ", killed=" + killed
                + ", filesCopied=" + filesCopied
                + ", dirCopied=" + dirCopied
                + ", bytesCopied=" + bytesCopied
                + ", timeTaken=" + timeTaken
                + ", unit='" + unit + '\''
                + ", jobProgress=" + jobProgress
                + '}';
    }
}
