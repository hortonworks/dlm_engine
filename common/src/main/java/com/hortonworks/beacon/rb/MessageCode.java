/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.rb;

/**
 * Message ID map enum.
 */
public enum MessageCode {
    COMM_000001("Missing parameter : {0}"),
    COMM_000002("Unable to parse retention age limit : {0}"),

    MAIN_000001("Submit successful {0} : {1}"),
    MAIN_000002("Exception while sync delete policy to remote cluster : {0}."),
    MAIN_000003("Exception while obtain replication type : "),

    ENTI_000001("HCFS to HCFS replication is not allowed"),
    ENTI_000002("Either sourceCluster or targetCluster should be same as local cluster name : {0}"),

    COMM_010001("{0} {1} cannot be null or empty");

    private final String msg;

    MessageCode(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }
}
