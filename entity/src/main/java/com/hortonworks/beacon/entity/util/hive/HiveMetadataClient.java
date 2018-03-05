/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.entity.util.hive;

import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * Hive Metadata client interface.
 */
public interface HiveMetadataClient {
    void close() throws BeaconException;

    List<String> listDatabases() throws BeaconException;

    Path getDatabaseLocation(String dbName) throws BeaconException;

    List<String> getTables(String dbName) throws BeaconException;

    List<String> getFunctions(String dbName) throws BeaconException;

    boolean doesDBExist(String dbName) throws BeaconException;

    void dropTable(String dbName, String tableName) throws BeaconException;

    void dropDatabase(String dbName) throws BeaconException;

    void dropFunction(String dbName, String functionName) throws BeaconException;
}
