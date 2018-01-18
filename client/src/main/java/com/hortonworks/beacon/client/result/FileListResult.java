/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
                    + '}';
        }
    }
}
//RESUME CHECKSTYLE CHECK VisibilityModifierCheck

