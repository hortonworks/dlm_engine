package com.hortonworks.beacon.service;

import com.hortonworks.beacon.exceptions.BeaconException;

public interface BeaconService {

    String getName();

    void init() throws BeaconException;

    void destroy() throws BeaconException;
}

