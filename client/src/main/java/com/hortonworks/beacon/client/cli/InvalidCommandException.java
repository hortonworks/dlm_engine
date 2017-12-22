/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */


package com.hortonworks.beacon.client.cli;

/**
 * Exception used when CLI command is invalid.
 */
public class InvalidCommandException extends Exception {
    public InvalidCommandException(String message) {
        super(message);
    }
}
