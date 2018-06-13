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

package com.hortonworks.beacon.client.result;

import com.hortonworks.beacon.client.resource.APIResult;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * FileListResult is output returned after listing details from FS path.
 */

//SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
@XmlRootElement
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class FileListResult extends APIResult {

    @XmlElement
    public long totalResults;

    @XmlElement
    public FileList[] fileList;

    public FileListResult() {
    }

    public FileListResult(Status status, String message) {
        super(status, message);
    }

    private FileList[] getFileDetails() {
        return fileList;
    }

    private void setFileListCollection(FileList[] fileList, int size) {
        this.totalResults = size;
        this.fileList = fileList;
    }

    public long getTotalResults() {
        return totalResults;
    }

    @Override
    public Object[] getCollection() {
        return getFileDetails();
    }

    @Override
    public void setCollection(Object[] items) {
        if (items == null) {
            setFileListCollection(new FileList[0], 0);
        } else {
            FileList[] fileList = new FileList[items.length];
            for (int index=0; index<items.length; index++) {
                fileList[index] = (FileList) items[index];
            }
            setFileListCollection(fileList, fileList.length);
        }
    }


    /**
     * File List.
     */
    @XmlRootElement(name = "filelist")
    public static class FileList {
        @XmlElement
        public Long accessTime;

        @XmlElement
        public Long blockSize;

        @XmlElement
        public String group;

        @XmlElement
        public Long length;

        @XmlElement
        public Long modificationTime;

        @XmlElement
        public String owner;

        @XmlElement
        public String pathSuffix;

        @XmlElement
        public String permission;

        @XmlElement
        public Short replication;

        @XmlElement
        public String type;

        @XmlElement
        public boolean isEncrypted;

        @XmlElement
        public String encryptionKeyName;

        @XmlElement
        public boolean snapshottable;

        @Override
        public String toString() {
            return "FileList{"
                    + "accessTime=" + accessTime
                    + ", blockSize=" + blockSize
                    + ", group='" + group + '\''
                    + ", length=" + length
                    + ", owner='" + owner + '\''
                    + ", modificationTime=" + modificationTime
                    + ", pathSuffix='" + pathSuffix + '\''
                    + ", permission='" + permission + '\''
                    + ", replication=" + replication
                    + ", type='" + type + '\''
                    + ", isEncrypted='" + isEncrypted + '\''
                    + ", encryptionKeyName='" + encryptionKeyName + '\''
                    + ", snapshottable='" + snapshottable + '\''
                    + '}';
        }
    }
}
//RESUME CHECKSTYLE CHECK VisibilityModifierCheck

