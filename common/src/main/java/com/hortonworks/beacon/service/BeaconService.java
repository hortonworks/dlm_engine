package com.hortonworks.beacon.service;

import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * Created by sramesh on 9/30/16.
 */

public interface BeaconService {

    String getName();

    void init() throws BeaconException;

    void destroy() throws BeaconException;
}

